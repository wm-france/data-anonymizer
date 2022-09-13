import pandas as pd
from cryptography.fernet import Fernet

pd.options.mode.chained_assignment = None

# Input path
table_path = input("Input full file path and press Enter: \n> ")
# Retrieve folder path to place resulting file in same folder
folder_path = table_path.rpartition("/")[0]

# Load data from csv or excel to dataframe whether
if table_path.split(".")[1] == "xls" or table_path.split(".")[1] == "xlsx":
    table_to_encrypt = pd.read_excel(table_path, header=0)
elif table_path.split(".")[1] == "csv":
    table_to_encrypt = pd.read_csv(table_path)
else:
    raise ValueError(
        "File type incorrect. File needs to be of type csv, xls or xlsx."
    )

# Returns list of columns that contain ISRCs,
# and the first row where an IRSC was located in those columns


def isrc_column_identification(df_subject):
    nb_columns = len(df_subject.columns)
    nb_rows = len(df_subject)
    # store columns that have been identified as isrc
    isrc_columns = []
    isrc_first_row = []
    for col in range(nb_columns):
        # if one row is an ISRC, the column is considered to be an ISRC
        for row in range(nb_rows):
            element = df_subject.iloc[row, col]
            # nomenclature compliance test
            if (
                type(element) == str
                and len(element) == 12
                and element[0:2].isalpha()
                and element[2:5].isalnum()
                and element[5:].isnumeric()
            ):
                isrc_columns.append(col)
                isrc_first_row.append(row)
                break
    return [isrc_columns, isrc_first_row]


key = Fernet.generate_key()
f = Fernet(key)


def encrypt_isrc(isrc, encrypted_isrc_library):
    # if we have not already encrypted this ISRC
    if isrc not in encrypted_isrc_library:
        encrypted_isrc = f.encrypt(bytes(isrc, encoding="utf-8")).decode(
            "utf-8"
        )
        # if the encrypted value is already used for another ISRC
        while encrypted_isrc in encrypted_isrc_library.values():
            # generate new encrypted value
            encrypted_isrc = f.encrypt(bytes(isrc, encoding="utf-8")).decode(
                "utf-8"
            )
        # add to library
        encrypted_isrc_library[isrc] = encrypted_isrc
        return encrypted_isrc
    else:
        return encrypted_isrc_library[isrc]


def detect_encrypt_isrc(df_subject):
    isrc_columns_rows = isrc_column_identification(df_subject)
    isrc_columns = isrc_columns_rows[0]
    isrc_first_row = min(isrc_columns_rows[1])
    df_data_subject = df_subject.iloc[isrc_first_row:, :]
    # Correct the column naming, if data doesn't start at line 0 of file.
    if isrc_first_row > 0:
        df_data_subject.columns = df_subject.iloc[isrc_first_row - 1]
    encrypted_isrc_library = {}
    for col in isrc_columns:
        # asks user if they want to encrypt column
        col_name = df_data_subject.columns[col]
        confirmation_result = input(
            "Encrypt following column: " + col_name + "? (Y/N)\n> "
        ).upper()
        if confirmation_result != "Y" and confirmation_result != "N":
            raise ValueError("Not 'Y/N' entry.")
        if confirmation_result == "Y":
            # changes isrcs for corresponding encrypted values
            df_data_subject.loc[:, col_name] = df_data_subject.loc[
                :, col_name
            ].map(lambda x: encrypt_isrc(str(x), encrypted_isrc_library))
    df_subject.iloc[isrc_first_row:, :] = df_data_subject
    return df_subject


# applies encryption on imported table
encrypted_df = detect_encrypt_isrc(table_to_encrypt)

# stores resulting encrypted table in csv file
encrypted_table_path = table_path.split(".")[0] + "_encrypted.csv"
encrypted_df.to_csv(path_or_buf=encrypted_table_path)
