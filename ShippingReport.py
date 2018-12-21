import pyodbc as sql
import openpyxl
from datetime import datetime


class ShippingReport(object):
    def __init__(self):
        pass

    def extract_sql_data_generic(self, sql_data, level_titles, amount_col_name, discount_col_name, extra_col_names=[],
                                 combiners={}):
        compiled_data = []
        for x in range(5):
            raw_data = sql_data[x]
            columns = raw_data[0]
            row_data = raw_data[1]
            amount_col = columns.index(amount_col_name)
            discount_col = columns.index(discount_col_name)
            level_cols = [columns.index(level_titles[x]) for x in range(len(level_titles))]
            no_data = ["No " + x for x in level_titles]  # needs to be changed to input for more genereic
            results = {}
            total_amount = 0
            total_discount = 0
            for row in row_data:
                combination = ['-' for x in level_cols]
                combination = tuple(combination)
                amount = (float(abs(row[amount_col])))
                discount = (float(abs(row[discount_col])))
                total_amount += amount
                total_discount += discount
                for x in range(len(level_cols)):
                    combination = list(combination)
                    combination[x] = (row[level_cols[x]])
                    if combination[x] == "":
                        combination[x] = no_data[x]
                        # the next code snippet combines data from specific columns so that unnessasary variety is lumped together
                    for comb in combiners:
                        if comb in level_titles:
                            comb_col = level_titles.index(comb)
                            for comb_package in combiners[comb]:
                                if combination[comb_col] in combiners[comb][comb_package]:
                                    combination[comb_col] = comb_package
                    combination = tuple(combination)
                    try:
                        results[combination][
                            0] += amount  # combination variable is the combination of category codes, this is the billable amount of each line
                        results[combination][1] += discount  # this is the nonbillable amount of each line
                        for ex_num, extra in enumerate(extra_col_names):
                            results[combination][2 + ex_num] = row[
                                columns.index(extra)]  # this is the nonbillable amount of each line
                    except KeyError:  # if code does not yet exist in dict, add it initializing the values for that first entry
                        comb_array = [amount, discount]
                        for ex_num, extra in enumerate(extra_col_names):
                            comb_array.append(row[columns.index(extra)])  # this is the nonbillable amount of each line
                        results[combination] = comb_array
            total_tuple_as_array = ['-' for x in range(len(level_cols) + 1)]
            total_tuple_as_array[0] = "TOTAL"
            total_array = [total_amount, total_discount]
            for extra in extra_col_names:
                total_array.append(extra)
            results[tuple(total_tuple_as_array)] = total_array
            # the results dictionary will have each code combination as a key, with the values being
            # a list in which the first value is the billable charge and the second is the non-billable charge
            filtered_results = deepcopy(results)
            # the following for loop can filter out unwatend results. Ill keep this functionality for future use
            for key, value in results.items():
                break
                if nonzero <= 1:
                    if value[nonzero] == 0:
                        del filtered_results[key]
            # filtered results are a dictionary with only the only data used from the original sql data is the levels, amounts and discounts
            compiled_data.append(results)

        return compiled_data

    def date_limits(self, year_in=0, quarter=0):
        # returns the date as a string in the format of the sql table
        if year_in == 0:
            year = int(datetime.date.today().year)
        else:
            year = year_in

        q1 = "'%s-01-01 00:00'" % str(year)
        q2 = "'%s-04-01 00:00'" % str(year)
        q3 = "'%s-07-01 00:00'" % str(year)
        q4 = "'%s-10-01 00:00'" % str(year)
        q1_next = "'%s-01-01 00:00'" % str(year + 1)

        if quarter == 1:
            start_date = q1
            end_date = q2
        elif quarter == 2:
            start_date = q2
            end_date = q3
        elif quarter == 3:
            start_date = q3
            end_date = q4
        elif quarter == 4:
            start_date = q4
            end_date = q1_next
        elif quarter == 5:
            start_date = q1
            end_date = q1_next

        return [start_date, end_date]

    def sql_import_jobs(self, date_limits):
        # imports the necessary data for the report
        # dates format is in datetime.datetime(YYYY,MM,DD,HH,MI)
        # Invoice table as a backup, in case ledger entries does not get fixed
        invoice_table = "[Triangle Go Live Company$Service Invoice Line]"
        customer_table = "[Triangle Go Live Company$Customer]"
        # columns from invoice table included in case ledger entries no good
        invoice_columns = ["Document No_", "No_", "Service Item No_", "Description", "Work Type Code", "Posting Date",
                           "Quantity", "Unit of Measure Code", "Unit Price", "Warranty", "Amount", "Type",
                           "Unit Cost (LCY)", "Line Discount _", "Line Discount Amount",
                           "Fault Area Code", "Symptom Code", "Fault Reason Code", "Resolution Code", "Fault Code"]

        columns_formatted = "".join([", {0}.[" + c + "]" for c in invoice_columns])
        query_columns = """
                SELECT {0}.[Customer No_], {1}.[Name] AS 'Customer', {1}.[County] AS 'State', {1}.[City]
                """ + columns_formatted

        query_join = """
                FROM {0}
                INNER JOIN {1} 
                ON {0}.[Customer No_]={1}.[No_]"""

        # specify filters: work type codes and dates.
        # work type codes were copied from NAV, so there will be some string manipulation necessary to turn it into
        # proper comparison format. I did it this way to easily mirror between NAV and pure SQL
        work_type_codes = "DT|EE|ENGHOL|ENGSTD|ENGTRVL|ENGTRVLH|ENGUSHOL|ENGUSOT|ENGUSSAT|ENGUSSTD|INSTALL|INTOT|INTRH" \
                          "|INTSAT|INTSTD|INTTRH|INTTRV|LAYOVER|LAYOVER2|ME|PP SERVICE|SE|SFR|TECHUSOT|OT|TECHUSSAT" \
                          "|TECHUSSTD|TECHUSTRH|TECHUSTRV|TRG|US|TRAVEL|SERVICE|LIV EXPENS"
        work_type_filter_formatted = " OR".join(
            [" {0}.[Work Type Code] = " + "'" + c + "'" for c in work_type_codes.split("|")])
        query_filter = """
                WHERE {0}.[Posting Date] >= CONVERT(datetime, {2}, 20) AND {0}.[Posting Date] < CONVERT(datetime, {3}, 20) AND  {0}.[Type] = 2 AND 
                ({0}.[Responsibility Center] = 'SERVICE') ORDER BY {1}.[Name]"""  # + \
        # "AND ({0}.[Amount] > 0)"

        query = query_columns.format(invoice_table, customer_table) + query_join.format(invoice_table, customer_table) \
                + query_filter.format(invoice_table, customer_table, date_limits[0], date_limits[1])

        server = r'triangle-sql1\nav'
        database = r'TRIANGLE-LIVE'
        driver = r'{SQL Server}'
        conn = sql.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database)
        cursor = conn.cursor()

        cursor.execute(str(query))
        column_headers = [column[0] for column in cursor.description]

        row = cursor.fetchone()
        raw_data = []
        row_no = 1
        while row:
            pot_row = list(row)
            row = cursor.fetchone()
            raw_data.append(pot_row)
            row_no += 1
        # the point of outputting the raw data then modifying it slightly is to have a reference of the raw data in a ordered form.
        return [column_headers, raw_data]

    def sql_import_orders(self, date_limits):
        # imports the necessary data for the report
        # dates format is in datetime.datetime(YYYY,MM,DD,HH,MI)
        # Invoice table as a backup, in case ledger entries does not get fixed
        invoice_table = "[Triangle Go Live Company$Service Invoice Line]"
        customer_table = "[Triangle Go Live Company$Customer]"
        # columns from invoice table included in case ledger entries no good
        invoice_columns = ["Document No_", "No_", "Service Item No_", "Description", "Work Type Code", "Posting Date",
                           "Quantity", "Unit of Measure Code", "Unit Price", "Warranty", "Amount", "Type",
                           "Unit Cost (LCY)", "Line Discount _", "Line Discount Amount",
                           "Fault Area Code", "Symptom Code", "Fault Reason Code", "Resolution Code", "Fault Code"]

        columns_formatted = "".join([", {0}.[" + c + "]" for c in invoice_columns])
        query_columns = """
                SELECT {0}.[Customer No_], {1}.[Name] AS 'Customer', {1}.[County] AS 'State', {1}.[City]
                """ + columns_formatted

        query_join = """
                FROM {0}
                INNER JOIN {1} 
                ON {0}.[Customer No_]={1}.[No_]"""

        # specify filters: work type codes and dates.
        # work type codes were copied from NAV, so there will be some string manipulation necessary to turn it into
        # proper comparison format. I did it this way to easily mirror between NAV and pure SQL
        work_type_codes = "DT|EE|ENGHOL|ENGSTD|ENGTRVL|ENGTRVLH|ENGUSHOL|ENGUSOT|ENGUSSAT|ENGUSSTD|INSTALL|INTOT|INTRH" \
                          "|INTSAT|INTSTD|INTTRH|INTTRV|LAYOVER|LAYOVER2|ME|PP SERVICE|SE|SFR|TECHUSOT|OT|TECHUSSAT" \
                          "|TECHUSSTD|TECHUSTRH|TECHUSTRV|TRG|US|TRAVEL|SERVICE|LIV EXPENS"
        work_type_filter_formatted = " OR".join(
            [" {0}.[Work Type Code] = " + "'" + c + "'" for c in work_type_codes.split("|")])
        query_filter = """
                WHERE {0}.[Posting Date] >= CONVERT(datetime, {2}, 20) AND {0}.[Posting Date] < CONVERT(datetime, {3}, 20) AND  {0}.[Type] = 2 AND 
                ({0}.[Responsibility Center] = 'SERVICE') ORDER BY {1}.[Name]"""  # + \
        # "AND ({0}.[Amount] > 0)"

        query = query_columns.format(invoice_table, customer_table) + query_join.format(invoice_table, customer_table) \
                + query_filter.format(invoice_table, customer_table, date_limits[0], date_limits[1])

        server = r'triangle-sql1\nav'
        database = r'TRIANGLE-LIVE'
        driver = r'{SQL Server}'
        conn = sql.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database)
        cursor = conn.cursor()

        cursor.execute(str(query))
        column_headers = [column[0] for column in cursor.description]

        row = cursor.fetchone()
        raw_data = []
        row_no = 1
        while row:
            pot_row = list(row)
            row = cursor.fetchone()
            raw_data.append(pot_row)
            row_no += 1
        # the point of outputting the raw data then modifying it slightly is to have a reference of the raw data in a ordered form.
        return [column_headers, raw_data]
