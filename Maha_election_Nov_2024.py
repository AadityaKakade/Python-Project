import pandas as pd
import os
import utilities
from time import sleep
from os import system, name

MENUS = [
    "1. Download Data",
    "2. Load Data into DataFrame",
    "3. Show Winning Parties",
    "4. Show Winning Parties using PIE Chart",
    "5. Save to File",
    "6. Save to Oracle Database",
    "9. Exit"]

data_frame = pd.DataFrame()


def start_here() :
    while True :
        os.system('cls')
        for menu in MENUS :
            print(menu)

        choice = input("Select the option : ")

        if len(choice.strip()) > 0 :
            choice = int(choice)

        ## Download data from Election Commission WebSite
        if choice == 1 :
            utilities.download_and_save_data()
            print("Download Complete")
            sleep(3)

        ## Load data into Dataframe
        elif choice == 2 :
            data_frame = utilities.load_data_into_dataframe()
            print("Load Complete")
            sleep(4)

        ## Show Winning Parties
        elif choice == 3 :
            utilities.show_winning_parties(data_frame)
            sleep(4)

        ## Show Winning Parties oin PIE Chart
        elif choice == 4 :
            utilities.show_winning_parties_pie(data_frame)

        ## Save Winning Data to Excel File
        elif choice == 5 :
            utilities.save_data_to_excel_file(data_frame)
            print("Dataframe Saved to file successfully!")
            sleep(4)

        ## Save Raw Data to Oracle Database
        elif choice == 6 :
            utilities.save_to_oracle(data_frame)
            sleep(4)

        ## Exit
        elif choice == 9 :
            print("Exiting.. Bye!")
            break


if __name__ == "__main__" :
    start_here()
