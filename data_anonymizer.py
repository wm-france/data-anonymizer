import pandas as pd
from cryptography.fernet import Fernet
from os.path import exists

pd.options.mode.chained_assignment = None


# Find the headers line in dataframe and renames the columns accordingly
def clean_data(df_subject):
    list_unnamed_cols = df_subject.columns.str.match("Unnamed")
    count_unnamed_cols = sum(list_unnamed_cols)
    number_cols = len(raw_table_to_encrypt.columns)
    percent_unnamed_cols = count_unnamed_cols / number_cols
    # If more than 20% of the column names are empty
    if percent_unnamed_cols > 0.2:
        # Get the index of the row with the least null values
        # Use it as header
        headers_row = raw_table_to_encrypt.isnull().sum(axis=1).idxmin()
        clean_df_subject = df_subject.loc[headers_row + 1 :, :]
        clean_df_subject.columns = df_subject.loc[headers_row]
    else:
        clean_df_subject = df_subject
    # Drop all null columns
    clean_df_subject = clean_df_subject.dropna(axis=1).reset_index(drop=True)
    return clean_df_subject


# Returns list of columns that contain ISRCs,
# and the first row where an ISRC was located in those columns


def encrypt_series(s: pd.Series, encryption: Fernet) -> pd.Series:
    """Return encrypted version of Pandas Series."""
    # Convert to bytes because that's how Fernet works
    s = s.astype(str).str.encode("utf8")

    # Unique value -> encrypted value mapping Series
    map = pd.Series(b"", index=s.drop_duplicates())

    # Generate new encrypted keys until we have no duplicates
    while (duplicated := map.duplicated(keep=False)).any():
        map[duplicated] = map[duplicated].index.map(encryption.encrypt)

    # Applying mapping to input Series
    return s.map(map).str.decode("utf8")


def encrypt_df(df_subject, encryption):
    column_names_to_encrypt = []
    # Ask user which columns to encrypt
    col_names = df_subject.columns
    number_of_columns = len(col_names)
    result = input(
        "Columns : \n"
        + "".join(
            str(i) + ": " + col_names[i] + "\n"
            for i in range(number_of_columns)
        )
        + "Enter index of columns to encrypt (Ex : 1,4,7) \n> "
    ).upper()
    # Parse response
    result = result.replace(" ", "")
    if result == "":
        print("No columns to encrypt.")
        exit()
    entries = result.split(",")
    for entry in entries:
        if entry.isnumeric():
            col_index = int(entry)
            if 0 <= col_index <= number_of_columns:
                column_names_to_encrypt.append(col_names[int(entry)])
            else:
                raise ValueError("Entry out of range.")
        else:
            raise ValueError("Invalid entry.")

    # Replace values with corresponding encrypted values in chosen columns
    for col_name in column_names_to_encrypt:
        print(f"Encrypting {col_name}")
        df_subject[col_name] = encrypt_series(df_subject[col_name], encryption)

    return df_subject


def generate_encryption() -> Fernet:
    key = Fernet.generate_key()
    f = Fernet(key)
    return f


if __name__ == "__main__":
    # Input path
    table_path = input("Input full file path and press Enter: \n> ")
    # Retrieve folder path to place resulting file in same folder
    folder_path = table_path.rpartition("/")[0]
    # Load data from csv or excel to dataframe whether
    if exists(table_path):
        if (
            table_path.split(".")[1] == "xls"
            or table_path.split(".")[1] == "xlsx"
        ):
            raw_table_to_encrypt = pd.read_excel(table_path, header=0)
        elif table_path.split(".")[1] == "csv":
            raw_table_to_encrypt = pd.read_csv(table_path)
        else:
            raise ValueError(
                "File type incorrect. "
                "File needs to be of type csv, xls or xlsx."
            )
    else:
        raise ValueError("File does not exist.")

    encrypted_df = encrypt_df(
        clean_data(raw_table_to_encrypt), generate_encryption()
    )

    # Store resulting encrypted table in csv file
    encrypted_table_path = table_path.split(".")[0] + "_encrypted.csv"
    print(f"Saving result file to {encrypted_table_path}")
    encrypted_df.to_csv(
        path_or_buf=encrypted_table_path, index=False, encoding="utf-8"
    )
