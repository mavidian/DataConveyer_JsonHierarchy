# DataConveyer_JsonHierarchy

DataConveyer_JsonHierarchy is a console application to demonstate some ways Data Conveyer can be used to
transfor hierarchical JSON data. The application recognizes 3 alternative formats (JSON hierarchies) of the same data
representing population, drivers and vehicles by US state by years:

**Flat Data**
```
[
  {
    "State": "Alabama",
    "Year": "2009",
    "Population": "",
    "Drivers": "3782284",
    "Vehicles": "4610850"
  },
...
```

**Data by Year**
```
[
  {
    "State": "Alabama",
    "DataByYear": [
      {
        "2009": {
          "Population": "",
          "Drivers": "3782284",
          "Vehicles": "4610850"
        }
      },
      {
        "2010": {
          "Population": "4785514",
...
```

**Data by Type**
```
[
  {
    "State": "Alabama",
    "DataByType": [
      {
        "Population": {
          "2009": "",
          "2010": "4785514",
          ...
        }
      },
      {
        "Drivers": {
          "2009": "3782284",
...
```

The files presented on input are expected to be in one of these 3 formats and named with the respective suffixes of
*_FlatData.json*, *_DataByYear.json* and *_DataByType.json*. The application converts such files according to the following scheme:
   
    FlatData   ->  DataByYear
    DataByYear ->  DataByType
    DataByType ->  FlatData

There are sample input files located in the Data subfolder. They contain data on population, drivers and vehicles by US states.
The data was collected from public sources, such as https://www.census.gov/ or https://www.bts.gov/.

## Installation

* Fork this repository and clone it onto your local machine, or

* Download this repository onto your local machine.

## Usage

1. Open DataConveyer_JsonHierarchy solution in Visual Studio.

2. Build and run the application, e.g. hit F5.

    - A console window with directions will show up.

3. Copy an input file into the ...Data\In folder.

    - A message that the file was detected will appear in the console window.

4. Hit the spacebar to start the conversions.

    - The file will get processed as reported in the console window.

5. Review the contents of the output files placed in the ...Data\Out folder.

6. (optional) Repeat steps 3-5 for other additional input file(s).

7. To exit application, hit Enter key into the console window.


## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)

## Copyright

```
Copyright © 2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
```
