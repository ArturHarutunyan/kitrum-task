The main functionality of the class is to process input csv files and create an output csv file with total number of  sales.<br>
For solving this problem this algorithm is created.<br>
Data will get from the csv file via chunks(chunk is the maximal number of raws which the RAM can store).<br>
The chunk size is a parameter for the class.<br>
The class process the chunk data and stores the sum in a file, name of which is the department name.
(_Notice. use ext4 file system if the department count is larger._)<br>
After processing all chunks, the program reads from all tmp files and adds it in the output csv file.<br>


input file format


New York | 2020-01-01 | 100
--- | --- | ---
Boston | 2020-01-01 | 400
New York | 2020-01-02 | 20


output file format

New York | 120
--- | ---
Boston | 400

usage

```
    DataProcessor(
        chunk_size=10 ** 10,
        tmp_dir='tmp'
    ).process_data(
        input_file_name="in",
        out_file_name='out'
    )
```

The computational complexity of algorithm is ``O(N)`` where N is the count of elements in input csv file

the ``chunk_size`` parameter depends on the device's RAM. 
The parameter ``chunk_size`` represents the number of rows that will be processed during each computation.