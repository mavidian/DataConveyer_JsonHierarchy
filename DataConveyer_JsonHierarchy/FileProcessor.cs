// Copyright © 2019-2020 Mavidian Technologies Limited Liability Company. All Rights Reserved.

using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using System;
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
            TransformerType =  TransformerType.Clusterbound,
            OutputDataKind = KindOfTextData.UnboundJSON,
            XmlJsonOutputSettings = "IndentChars|  ",  // pretty print
            OutputFileName = outLocation + Path.DirectorySeparatorChar + outFileName
         };

         switch (conversionMode)
         {
            case ConversionMode.Flat_to_ByYear:
               config.ClusterMarker = (r, pr, i) => pr == null || (string)pr["State"] != (string)r["State"];  // cluster by state (intake records need to be sorted by state) - note casts to compare values, not references
               config.ClusterboundTransformer = ConvertFlatToByYear;
               break;
            case ConversionMode.ByYear_to_ByType:
               config.ClusterboundTransformer = ConvertByYearToByType;
               break;
            case ConversionMode.ByType_to_Flat:
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
      {
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

      private ICluster ConvertByYearToByType(ICluster inClstr)
      {
         throw new NotImplementedException();
      }

      private ICluster ConvertByTypeToFlat(ICluster inClstr)
      {
         throw new NotImplementedException();
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
               return (ConversionMode.Flat_to_ByYear, OutName: Regex.Replace(inputName, _trailerPattern, "_DataByType.json"));
            case "_databytype.json":
               return (ConversionMode.Flat_to_ByYear, OutName: Regex.Replace(inputName, _trailerPattern, "_FlatData.json"));
            default:
               return (ConversionMode.Undetermined, OutName: null);
         }
      }

   }
}
