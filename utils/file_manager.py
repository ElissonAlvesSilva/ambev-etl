import json
import os


class FileManager:
    """
    Centralizes all the read and write from files.
    """

    @staticmethod
    def create_if_dont_exist(filepath):
        if not os.path.exists(filepath):
            os.system('mkdir -p ' + filepath)

    @staticmethod
    def read_from_json_file(filepath, filename):
        filepath_final = FileManager.set_path(filepath, filename)
        if os.path.exists(filepath_final):
            with open(filepath_final) as json_data:
                data = json.load(json_data)
            return data

    @staticmethod
    def write_json_to_file(filepath, filename, arrayjson):
        filepath_final = FileManager.set_path(filepath, filename)
        FileManager.clear_file(filepath_final)
        with open(filepath_final, 'a') as outfile:
            json.dump(arrayjson, outfile)
            outfile.write('\n')

    @staticmethod
    def write_string_to_file(filepath, filename, content):
        filepath_final = FileManager.set_path(filepath, filename)
        FileManager.clear_file(filepath_final)
        with open(filepath_final, 'a') as outfile:
            outfile.write(content)

    @staticmethod
    def clear_file(filename):
        open(filename, 'w').close()

    @staticmethod
    def set_path(filepath, filename):
        if filepath.endswith('/'):
            return filepath + filename
        else:
            return filepath + '/' + filename
