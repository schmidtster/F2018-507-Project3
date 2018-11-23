import sqlite3 as sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


def clean_database():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT,
            'SpecificBeanBarName' TEXT,
            'REF' TEXT,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocationId' INT,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOriginId' INT
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INT,
            'Area' REAL
        );
    '''
    cur.execute(statement)
    conn.close()
    return "Database wiped"


# readCSV
def read_csv():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    with open(BARSCSV, encoding='utf-8') as csvDataFile:
        csvreader = csv.reader(csvDataFile)
        for row in csvreader:
            if row[0] != "Company":
                insertion = (row[0], row[1], row[2], row[3], row[4].replace("%", ""), row[5], row[6], row[7], row[8])
                statement = 'INSERT INTO "Bars" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, ' \
                            'CompanyLocationId, Rating, BeanType, BroadBeanOriginId) '
                statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
                cur.execute(statement, insertion)
    conn.commit()
    conn.close()
    return "CSV read"


def read_json():
    open_file = open(COUNTRIESJSON, 'r', encoding="utf-8")
    read_file = open_file.read()
    countries_list = json.loads(read_file)
    open_file.close()
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    for country in countries_list:
        insertion_json = (country["alpha2Code"], country["alpha3Code"], country["name"], country["region"],
                          country["subregion"], country["population"], country["area"])
        statement_json = 'INSERT INTO "Countries" (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) '
        statement_json += 'VALUES (?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement_json, insertion_json)
    conn.commit()
    conn.close()


def update_country_codes():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement = "SELECT Bars.CompanyLocationId, Countries.Id FROM Bars JOIN Countries WHERE Bars.CompanyLocationId = " \
                "Countries.EnglishName"
    execute = cur.execute(statement)
    results = execute.fetchall()
    for each_tuple in results:
        statement = 'UPDATE Bars SET CompanyLocationId = "{}" WHERE Bars.CompanyLocationId = "{}"'.format(each_tuple[1],
                                                                                                          each_tuple[0])
        cur.execute(statement)
        conn.commit()

    statement = "SELECT Bars.BroadBeanOriginId, Countries.Id FROM Bars JOIN Countries WHERE Bars.BroadBeanOriginId = " \
                "Countries.EnglishName"
    execute = cur.execute(statement)
    results = execute.fetchall()
    for each_tuple in results:
        statement = 'UPDATE Bars SET BroadBeanOriginId = "{}" WHERE Bars.BroadBeanOriginId = "{}"'.format(each_tuple[1],
                                                                                                          each_tuple[0])
        cur.execute(statement)
        conn.commit()
    conn.close()
    return "JSON read"


# 'bars sourceregion=Africa ratings top=5'
# Part 2: Implement logic to process user commands
def process_command(command):
    parameters = command.split()
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    if parameters[0] == "bars":
        column_name = {
            "sellcountry": ("c1.Alpha2", "Bars.CompanyLocationId"),
            "sourcecountry": ("c1.Alpha2", "Bars.BroadBeanOriginId"),
            "sellregion": ("c1.Region", "Bars.CompanyLocationId"),
            "sourceregion": ("c1.Region", "Bars.BroadBeanOriginId")}
        used_commands = ['AS c1 ON Bars.CompanyLocationId = c1.Id JOIN Countries AS c2 ON Bars.BroadBeanOriginId = '
                         'c2.Id', 'ORDER BY Bars.Rating DESC', 'LIMIT 10']
        for parameter_per_command in parameters[1:]:
            if "=" in parameter_per_command:
                parameter = parameter_per_command.split("=")
                if parameter[0] in ["sellcountry", "sourcecountry"]:
                    columns = column_name[parameter[0]]
                    used_commands[0] = "AS c1 ON {} = '{}' AND {} = c1.Id JOIN Countries as c2 ON c2.Id = {}" \
                                       "".format(columns[0], parameter[1], columns[1], columns[1])
                elif parameter[0] in ["sellregion", "sourceregion"]:
                    columns = column_name[parameter[0]]
                    used_commands[0] = "AS c1 ON {} = '{}' AND {} = c1.Id JOIN Countries as c2 ON c2.Id = {}" \
                                       "".format(columns[0], parameter[1], columns[1], columns[1])
                elif parameter[0] == "top":
                    top = parameter[1]
                    used_commands[2] = "LIMIT {}".format(int(top))
                elif parameter[0] == "bottom":
                    if "DESC" in used_commands[1]:
                        used_commands[1] = "ORDER BY Bars.Rating"
                    bottom = parameter[1]
                    used_commands[2] = "ASC LIMIT {}".format(int(bottom))
                else:
                    print("error")
                    # bogus parameter throw exception
            if "cocoa" == parameter_per_command:
                used_commands[1] = "ORDER BY Bars.CocoaPercent"
            if "ratings" == parameter_per_command:
                pass
            if "top" or "bottom" in parameter_per_command:
                pass
            else:
                print("error", parameter_per_command)
                # bogus parameter throw exception
        statement_bars = 'SELECT Bars.SpecificBeanBarName, Bars.Company, c1.EnglishName, Bars.Rating, ' \
                         'Bars.CocoaPercent, c2.EnglishName FROM Bars JOIN Countries {} {} {}' \
                         ''.format(used_commands[0], used_commands[1], used_commands[2])
        # print(statement_bars)
        execute = cur.execute(statement_bars)
        results_bars = execute.fetchall()
        return results_bars

    if parameters[0] == "companies":
        used_commands = ["AVG(Bars.Rating) AS average", "AS c1 ON c1.Id = Bars.CompanyLocationId",
                         "ORDER BY average DESC", "LIMIT 10"]
        column_name = {
            "country": ("c1.Alpha2", "Bars.CompanyLocationId"),
            "region": ("c1.Region", "Bars.CompanyLocationId")
        }
        for parameter_per_command in parameters[1:]:
            if "=" in parameter_per_command:
                parameter = parameter_per_command.split("=")
                if parameter[0] == "country":
                    columns = column_name[parameter[0]]
                    used_commands[1] = "AS c1 ON {} = '{}' AND {} = c1.Id JOIN Countries as c2 ON c2.Id = {}" \
                                       "".format(columns[0], parameter[1], columns[1], columns[1])
                elif parameter[0] == "region":
                    columns = column_name[parameter[0]]
                    used_commands[1] = "AS c1 ON {} = '{}' AND {} = c1.Id JOIN Countries as c2 ON c2.Id = {}" \
                                       "".format(columns[0], parameter[1], columns[1], columns[1])
                elif parameter[0] == "top":
                    top = parameter[1]
                    used_commands[3] = "LIMIT {}".format(int(top))
                elif parameter[0] == "bottom":
                    used_commands[2] = "ORDER BY average"
                    bottom = parameter[1]
                    used_commands[3] = "DESC LIMIT {}".format(int(bottom))
                else:
                    print("error")
                    # bogus parameter throw exception
            if "cocoa" == parameter_per_command:
                used_commands[0] = "AVG(Bars.CocoaPercent) AS average"
                used_commands[2] = "ORDER BY average DESC"
            if "bars_sold" == parameter_per_command:
                used_commands[0] = "COUNT(Bars.SpecificBeanBarName) AS count_bars"
                used_commands[2] = "ORDER BY COUNT(Bars.SpecificBeanBarName) DESC"
        statement_companies = 'SELECT Bars.Company, c1.EnglishName, {} FROM Bars JOIN Countries {} GROUP BY ' \
                              'Bars.Company HAVING COUNT(Bars.Company) > 4 {} {}' \
                              ''.format(used_commands[0], used_commands[1], used_commands[2], used_commands[3])
        # print(statement_companies)
        execute = cur.execute(statement_companies)
        results_companies = execute.fetchall()
        return results_companies

    if parameters[0] == "countries":
        used_commands = ["AVG(Bars.Rating) AS average", "AS c1 ON", "c1.Id = Bars.CompanyLocationId",
                         "Bars.CompanyLocationId", "average", "ORDER BY average DESC", "LIMIT 10"]
        column_name = {
            "sellers": ("c1.Region", "Bars.CompanyLocationId"),
            "sources": ("c1.Region", "Bars.BroadBeanOriginId")
        }
        for parameter_per_command in parameters[1:]:
            if "=" in parameter_per_command:
                parameter = parameter_per_command.split("=")
                if parameter[0] == "region":
                    used_commands[1] = "AS c1 ON c1.Region = '{}'".format(parameter[1])
                elif parameter[0] == "top":
                    if "DESC" not in used_commands[5]:
                        used_commands[5] += " DESC"
                    top = parameter[1]
                    used_commands[6] = "LIMIT {}".format(int(top))
                elif parameter[0] == "bottom":
                    used_commands[5] = "ORDER BY average"
                    bottom = parameter[1]
                    used_commands[6] = "ASC LIMIT {}".format(int(bottom))
                else:
                    print("error")
                    # bogus parameter throw exception
            if "sellers" == parameter_per_command:
                columns = column_name[parameter_per_command]
                used_commands[2] = "c1.Id = {}".format(columns[1])
                used_commands[4] = "COUNT(Bars.CompanyLocationId)"
            if "sources" == parameter_per_command:
                columns = column_name[parameter_per_command]
                used_commands[2] = "c1.Id = {}".format(columns[1])
                used_commands[3] = "Bars.BroadBeanOriginId"
                used_commands[4] = "COUNT(Bars.BroadBeanOriginId)"
            if "cocoa" == parameter_per_command:
                used_commands[0] = "AVG(Bars.CocoaPercent) AS average"
                used_commands[5] = "ORDER BY average DESC"
            if "bars_sold" == parameter_per_command:
                used_commands[0] = "COUNT(Bars.SpecificBeanBarName) AS count_bars"
                used_commands[5] = "ORDER BY count_bars"
                used_commands[4] = "count_bars"
            else:
                # print("error")
                continue
        statement_countries = 'SELECT c1.EnglishName, c1.Region, {} FROM Bars JOIN Countries {} {} GROUP BY {} ' \
                              'HAVING {} > 4 {} {}'.format(used_commands[0],
                                                           used_commands[1],
                                                           used_commands[2],
                                                           used_commands[3],
                                                           used_commands[4],
                                                           used_commands[5],
                                                           used_commands[6])
        # print(statement_countries)
        execute = cur.execute(statement_countries)
        results_countries = execute.fetchall()
        return results_countries

    if parameters[0] == "regions":
        used_commands = ["AVG(Bars.Rating) AS average", "Bars.CompanyLocationId", "average", "ORDER BY average DESC",
                         "LIMIT 10"]
        column_name = {
            "sellers": ("c1.Region", "Bars.CompanyLocationId"),
            "sources": ("c1.Region", "Bars.BroadBeanOriginId")
        }
        for parameter_per_command in parameters[1:]:
            if "=" in parameter_per_command:
                parameter = parameter_per_command.split("=")
                if parameter[0] == "top":
                    if "DESC" not in used_commands[3]:
                        used_commands[3] += " DESC"
                    top = parameter[1]
                    used_commands[4] = "LIMIT {}".format(int(top))
                elif parameter[0] == "bottom":
                    used_commands[3] = "ORDER BY average"
                    bottom = parameter[1]
                    used_commands[4] = "ASC LIMIT {}".format(int(bottom))
                else:
                    print("error")
                    # bogus parameter throw exception
            if "sellers" == parameter_per_command:
                columns = column_name[parameter_per_command]
                used_commands[1] = columns[1]
                used_commands[2] = "COUNT(Bars.CompanyLocationId)"
            if "sources" == parameter_per_command:
                columns = column_name[parameter_per_command]
                used_commands[1] = columns[1]
                used_commands[2] = "COUNT(Bars.BroadBeanOriginId)"
            if "cocoa" == parameter_per_command:
                used_commands[0] = "AVG(Bars.CocoaPercent) AS average"
                used_commands[3] = "ORDER BY average DESC"
            if "bars_sold" == parameter_per_command:
                used_commands[0] = "COUNT(Bars.SpecificBeanBarName) AS count_bars"
                used_commands[3] = "ORDER BY count_bars"
                used_commands[2] = "count_bars"
            else:
                # print("error")
                continue
        statement_regions = 'SELECT c1.Region, {} FROM Bars JOIN Countries AS c1 ON c1.Id = {} GROUP BY c1.Region ' \
                            'HAVING {} > 4 {} {}'.format(used_commands[0],
                                                         used_commands[1],
                                                         used_commands[2],
                                                         used_commands[3],
                                                         used_commands[4])
        # print(statement_regions)
        execute = cur.execute(statement_regions)
        results_regions = execute.fetchall()
        return results_regions


def load_help_text():
    with open('help.txt') as f:
        return f.read()


# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    clean_database()
    read_json()
    read_csv()
    update_country_codes()

    help_text = load_help_text()
    bars_commands = ["bars", "sellcountry", "sourcecountry", "sellregion", "sourceregion", "ratings", "cocoa", "top",
                     "bottom"]
    countries_commands = ["countries", "region", "sellers", "sources", "ratings", "cocoa", "bars_sold", "top", "bottom"]
    companies_commands = ["companies", "country", "region", "ratings", "cocoa", "bars_sold", "top", "bottom"]
    regions_commands = ["regions", "sellers", "sources", "ratings", "cocoa", "bars_sold", "top", "bottom"]
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        response_list = response.split()
        if bars_commands[0] in response_list:
            command_list = ["", "", "", ""]
            command_string = ""
            for each_parameter in response_list:
                if "=" in each_parameter:
                    params = each_parameter.split("=")
                    if params[0] in bars_commands[1:5]:
                        command_list[1] = each_parameter
                    if params[0] in bars_commands[7:9]:
                        command_list[3] = each_parameter
                elif each_parameter == bars_commands[0]:
                    command_list[0] = each_parameter
                elif each_parameter in bars_commands[5:7]:
                    command_list[2] = each_parameter
                else:
                    print("Command not recognized: ", response)
                    interactive_prompt()
            for params in command_list:
                if params != "":
                    command_string += " " + params
            command_processed = process_command(command_string)
            for each_result in command_processed:
                # pp.pprint(each_result)
                print("{:<30.30} | {:<30.30} | {:<25.25} | {:<5} | {:<5} | {:<25.25}".format(each_result[0],
                                                                                             each_result[1],
                                                                                             each_result[2],
                                                                                             each_result[3],
                                                                                             each_result[4],
                                                                                             each_result[5]))

        elif countries_commands[0] in response_list:
            command_list = ["", "", "", "", ""]
            command_string = ""
            print("yes")
            for each_parameter in response_list:
                if "=" in each_parameter:
                    params = each_parameter.split("=")
                    if params[0] == countries_commands[1]:
                        command_list[1] = each_parameter
                    if params[0] in countries_commands[7:9]:
                        command_list[3] = each_parameter
                elif each_parameter in countries_commands[2:4]:
                    command_list[2] = each_parameter
                elif each_parameter == countries_commands[0]:
                    command_list[0] = each_parameter
                elif each_parameter in countries_commands[4:7]:
                    command_list[3] = each_parameter
                else:
                    print("Command not recognized: ", response)
                    interactive_prompt()
            for params in command_list:
                if params != "":
                    command_string += " " + params
            command_processed = process_command(command_string)
            for each_result in command_processed:
                print("{:<30.30} | {:<25.25} | {:<5}".format(each_result[0], each_result[1], each_result[2]))

        elif companies_commands[0] in response_list:
            command_list = ["", "", "", ""]
            command_string = ""
            for each_parameter in response_list:
                if "=" in each_parameter:
                    params = each_parameter.split("=")
                    if params[0] in companies_commands[1:3]:
                        command_list[1] = each_parameter
                    if params[0] in companies_commands[6:8]:
                        command_list[3] = each_parameter
                elif each_parameter == companies_commands[0]:
                    command_list[0] = each_parameter
                elif each_parameter in companies_commands[3:6]:
                    command_list[2] = each_parameter
                else:
                    print("Command not recognized: ", response)
                    interactive_prompt()
            for params in command_list:
                if params != "":
                    command_string += " " + params
            command_processed = process_command(command_string)
            for each_result in command_processed:
                print("{:<30.30} | {:<25.25} | {:<5}".format(each_result[0], each_result[1], each_result[2]))

        elif regions_commands[0] in response_list:
            command_list = ["regions", "", "", ""]
            command_string = ""
            for each_parameter in response_list:
                if "=" in each_parameter:
                    params = each_parameter.split("=")
                    if params[0] in regions_commands[6:8]:
                        command_list[3] = each_parameter
                elif each_parameter in regions_commands[1:3]:
                    command_list[1] = each_parameter
                elif each_parameter == regions_commands[0]:
                    command_list[0] = each_parameter
                elif each_parameter in regions_commands[3:6]:
                    command_list[2] = each_parameter
                else:
                    print("Command not recognized: ", response)
                    interactive_prompt()
            for params in command_list:
                if params != "":
                    command_string += " " + params
            command_processed = process_command(command_string)
            for each_result in command_processed:
                print("{:<25.25} | {:<5}".format(each_result[0], each_result[1]))

        elif response == 'help':
            print(help_text)
            continue

        elif response == "exit":
            response = response
            print("Exiting the program...")

        else:
            print("Command not recognized: ", response)
            interactive_prompt()


# Make sure nothing runs or prints out when this file is run as a module
if __name__ == "__main__":
    interactive_prompt()
