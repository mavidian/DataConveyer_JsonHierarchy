// Copyright © 2019-2020 Mavidian Technologies Limited Liability Company. All Rights Reserved.

using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DataConveyer_JsonHierarchy
{
   /// <summary>
   /// Represents Data Conveyer functionality specific to processing hierarchical JSON data
   /// </summary>
   internal class FileProcessor
   {
      private enum ConversionMode
      {
         Undetermined,
         Flat_to_ByYear,
         ByYear_to_ByType,
         ByType_to_Flat
      }

      private readonly IOrchestrator Orchestrator;

      internal FileProcessor(string inFile, string outLocation)
      {
         (var conversionMode, var outFileName) = GetConversionAndOutputName(Path.GetFileName(inFile));
         var config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Transorming JSON hierarchy nesting.", LogEntrySeverity.Information))
         {
            ReportProgress = true,
            ProgressInterval = 1000,
            ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Intake) Console.Write($"\rProcessed {e.RecCnt:N0} records so far..."); },
            PhaseFinishedHandler = (s, e) => { if (e.Phase == Phase.Intake) Console.WriteLine($"\rProcessed {e.RecCnt:N0} records. Done!   "); },
            InputDataKind = KindOfTextData.UnboundJSON,
            InputFileName = inFile,
            AllowOnTheFlyInputFields = true,  // TODO: (in Data Conveyer) consider UnboundJSON ignoring this setting like X12 
            AllowTransformToAlterFields = true,  //IMPORTANT! otherwise null items will be produced!
            OutputDataKind = KindOfTextData.UnboundJSON,
            XmlJsonOutputSettings = "IndentChars|  ",  // pretty print
            OutputFileName = outLocation + Path.DirectorySeparatorChar + outFileName
         };

         switch (conversionMode)
         {
            case ConversionMode.Flat_to_ByYear:
               config.ClusterMarker = (r, pr, i) => pr == null || (string)pr["State"] != (string)r["State"];  // cluster by state (intake records need to be sorted by state) - note casts to compare values, not references
               config.TransformerType = TransformerType.Clusterbound;
               config.ClusterboundTransformer = ConvertFlatToByYear;
               break;
            case ConversionMode.ByYear_to_ByType:
               config.TransformerType = TransformerType.Recordbound;
               config.RecordboundTransformer = ConvertByYearToByType;
               break;
            case ConversionMode.ByType_to_Flat:
               config.TransformerType = TransformerType.Clusterbound;
               config.ClusterboundTransformer = ConvertByTypeToFlat;
               break;
         }

         Orchestrator = OrchestratorCreator.GetEtlOrchestrator(config);
      }


      /// <summary>
      /// Execute Data Conveyer process.
      /// </summary>
      /// <returns>Task containing the process results.</returns>
      internal async Task<ProcessResult> ProcessFileAsync()
      {
         var result = await Orchestrator.ExecuteAsync();
         Orchestrator.Dispose();

         return result;
      }


      private ICluster ConvertFlatToByYear(ICluster inClstr)
      {  //each cluster contains flat records for a state; all records in a cluster get collapsed into a single record
         // Fields in DataByYear are: State, DataByYear[0].2009.Population, DataByYear[0].2009.Drivers, ..., DataByYear[11].2020.Vehicles  (counts/years may vary)
         var outClstr = inClstr.GetEmptyClone();
         var outRec = inClstr[0].GetEmptyClone();  // a single output record for the state
         outRec.AddItem("State", inClstr[0]["State"]);
         var yearIdx = 0;
         foreach(var rec in inClstr.Records.OrderBy(r => r["Year"]))
         {
            var year = rec["Year"];
            outRec.AddItem($"DataByYear[{yearIdx}].{year}.Population", rec["Population"]);
            outRec.AddItem($"DataByYear[{yearIdx}].{year}.Drivers", rec["Drivers"]);
            outRec.AddItem($"DataByYear[{yearIdx}].{year}.Vehicles", rec["Vehicles"]);
            yearIdx++;
         }
         outClstr.AddRecord(outRec);
         return outClstr;
      }

      private IRecord ConvertByYearToByType(IRecord inRec)
      {  //here, there is a 1 to 1 relationship between input and output records (a record per state), so RecordBound transformes is used
         // Fields in DataByType are: State, DataByType[0].Population.2009, DataByType[0].Population.2010, ...,  DataByType[0].Population.2020, DataByType[1].Drivers.2009, ..., DataByType[2].Vehicles.2020
         static string Get2ndToLastSegment(string key) => key.Split('.').Reverse().Skip(1).First();
         var outRec = inRec.GetEmptyClone();
         outRec.AddItem("State", inRec["State"]);
         foreach (var itm in inRec.Items.Where(i => i.Key.EndsWith("Population")))
         {  //note that items do not to be sorted by year here (UnboundJSON output provider will group them appropriately)
            var year = Get2ndToLastSegment(itm.Key);
            outRec.AddItem($"DataByType[0].Population.{year}", itm.Value);
         }
         foreach (var itm in inRec.Items.Where(i => i.Key.EndsWith("Drivers")))
         {  //like above, no sort by year is needed here
            var year = Get2ndToLastSegment(itm.Key);
            outRec.AddItem($"DataByType[1].Drivers.{year}", itm.Value);

         }
         foreach (var itm in inRec.Items.Where(i => i.Key.EndsWith("Vehicles")))
         {  //neither is sort by year needed here
            var year = Get2ndToLastSegment(itm.Key);
            outRec.AddItem($"DataByType[2].Vehicles.{year}", itm.Value);
         }
         return outRec;
      }


      private ICluster ConvertByTypeToFlat(ICluster inClstr)
      { // here, single record clusters on intake will get expanded, so that there is one record per year
         Debug.Assert(inClstr.Count == 1);
         var inRec = inClstr[0];  // single record in input cluster
         var outClstr = inClstr.GetEmptyClone();
         var state = inRec["State"];

         foreach (var yearGrp in inRec.Items.Where(i => i.Key != "State")
                                            .GroupBy(i => i.Key.Split('.')
                                            .Reverse().First()))  // year is last key segment
         {
            Debug.Assert(yearGrp.Count() == 3);  // Population, Drivers & Vehicles
            var year = yearGrp.Key;
            var outRec = inRec.GetEmptyClone();
            outRec.AddItem("State", state);
            outRec.AddItem("Year", year);
            IItem AddItemToOutput(string key) => outRec.AddItem(key, yearGrp.FirstOrDefault(i => i.Key.Contains(key)).Value);
            AddItemToOutput("Population");
            AddItemToOutput("Drivers");
            AddItemToOutput("Vehicles");
            outClstr.AddRecord(outRec);
         }

         return outClstr;
      }


      private static readonly string _trailerPattern = "_[^_]*$";
      private static readonly Regex _trailerRegex = new Regex(_trailerPattern);
      /// <summary>
      /// Determine conversion mode by matching its trailing part; also determine the output file name.
      /// </summary>
      /// <param name="inputName"></param>
      /// <returns></returns>
      private static (ConversionMode ConversionMode, string OutName) GetConversionAndOutputName(string inputName)
      {
         var trailerMatch = _trailerRegex.Match(inputName);
         if (!trailerMatch.Success) return (ConversionMode.Undetermined, OutName: null);
         switch (trailerMatch.Value.ToLower())
         {
            case "_flatdata.json":
               return (ConversionMode.Flat_to_ByYear, OutName: Regex.Replace(inputName, _trailerPattern, "_DataByYear.json"));
            case "_databyyear.json":
               return (ConversionMode.ByYear_to_ByType, OutName: Regex.Replace(inputName, _trailerPattern, "_DataByType.json"));
            case "_databytype.json":
               return (ConversionMode.ByType_to_Flat, OutName: Regex.Replace(inputName, _trailerPattern, "_FlatData.json"));
            default:
               return (ConversionMode.Undetermined, OutName: null);
         }
      }

   }
}
