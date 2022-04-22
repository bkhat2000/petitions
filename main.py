from petitions import petitions

def main():
    source_path = "input_data_folder/input_data.json"
    destination_path = "output_data_folder/"

    petitions.Petitions(source_path, destination_path)


if __name__ == '__main__':
    main()