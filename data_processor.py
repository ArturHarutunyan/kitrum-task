import pandas as pd
import os


class DataProcessor:
    """
    Data processor class.
    The main functionality of the class is to process input csv files and create an output csv file with total number of
     sales.
    For solving this problem this algorithm is created.
        Data will get from the csv file via chunks(chunk is the maximal number of raws which the RAM can store).
         The chunk size is a parameter for the class.
        The class process the chunk data and stores the sum in a file, name of which is the department name.
            Notice. use ext4 file system if the department count is larger.
        After processing all chunks, the program reads from all tmp files and adds it in the output csv file.
    """

    def __init__(self, chunk_size: int = 10 ** 10, tmp_dir: str = 'tmp') -> None:
        """
        param chunk_size: the chunk_size for reading info from csv
        param tmp_dir: tmp directory name for creating tmp files
        """
        self.chunk_size = chunk_size
        self.tmp_dir = tmp_dir
        os.mkdir(tmp_dir)

    def __read_and_preprocess_file(self, input_file_name: str) -> None:
        """
        function for reading and preprocessing from the file.
        This function reads from csv file via chunks, groups by departments and calculates the sales sum.
        The sum of calculation will be written in the file.
        param input_file_name: Input file path
        """
        df = pd.read_csv(f'{input_file_name}.csv', chunksize=self.chunk_size,
                         names=('department', 'date', 'number_of_sales'))
        for chunk in df:
            group = chunk.groupby('department').sum()
            for _, row in group.iterrows():
                tmp_file_name = f'./{self.tmp_dir}/{row.name}'
                if os.path.exists(tmp_file_name):
                    with open(tmp_file_name, 'r+') as f:
                        old_number = f.read()
                        f.seek(0)
                        f.write(str(row.number_of_sales + int(old_number)))
                else:
                    with open(tmp_file_name, 'w') as f:
                        f.write(str(row.number_of_sales))

    def process_data(self, input_file_name: str, out_file_name: str) -> None:
        """
        This function receives an input file, processes it and store the output in the specified file.
        param input_file_name: Input file path
        param out_file_name: Output file path
        """
        self.__read_and_preprocess_file(input_file_name)
        with open(f'{out_file_name}.csv', 'w') as out_file:
            for file in os.scandir(self.tmp_dir):
                tmp_file_name = f'./{self.tmp_dir}/{file.name}'
                with open(tmp_file_name, 'r+') as f:
                    count_sum = f.read()
                    out_file.write(f'{file.name},{count_sum}\r\n')
                os.remove(tmp_file_name)

    def __del__(self) -> None:
        os.rmdir(self.tmp_dir)


if __name__ == '__main__':
    DataProcessor(
        chunk_size=10 ** 10,
        tmp_dir='tmp'
    ).process_data(
        input_file_name="in",
        out_file_name='out'
    )
