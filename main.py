
from api import Api
from database import Database
from reader import Reader

if __name__ == '__main__':
    database = Database()


    print(" -- Stock CSV reader -- ")


    with Api(database) as api:
        while True:
            print("Options:")
            print("1 - Load data from CSV file")
            print("2 - List and delete previous imports")
            option = input("Select option: ")
            if option == '1':
                filename = input("Filename: ")
                reader = Reader(api, filename)
                reader.run()
            elif option == '2':
                api.list_loads()

            run_again = input("Back to menu? (Y/N): ")

            if run_again == 'N':
                break
