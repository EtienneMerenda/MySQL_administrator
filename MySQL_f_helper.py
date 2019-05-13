# -*- encoding: utf-8 -*-

type_MySQL_help = {"Integral":
                    {"BIT[(M)]": "A bit-value type. M indicates the number of bits per value, from 1 to 64. The default is 1 if M is omitted",

                    "TINYINT[(M)] [UNSIGNED] [ZEROFILL]": "A very small integer. The signed range is -128 to 127. The unsigned range is 0 to 255.",
                    "BOOL": "These types are synonyms for TINYINT(1). A value of zero is considered false. Nonzero values are considered true",

                    "SMALLINT[(M)] [UNSIGNED] [ZEROFILL]": "A small integer. The signed range is -32768 to 32767. The unsigned range is 0 to 65535.",

                    "MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]": "A medium-sized integer. The signed range is -8388608 to 8388607. The unsigned range is 0 to 16777215.",

                    "INT[(M)] [UNSIGNED] [ZEROFILL]": "A normal-size integer. The signed range is -2147483648 to 2147483647. The unsigned range is 0 to 4294967295.",

                    "BIGINT[(M)] [UNSIGNED] [ZEROFILL]": "A large integer. The signed range is -9223372036854775808 to 9223372036854775807. The unsigned range is 0 to 18446744073709551615.",

                    "DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]": "A packed “exact” fixed-point number. M is the total number of digits (the precision) and D is the number of digits after the decimal"
                    "point (the scale). The decimal point and (for negative numbers) the - sign are not counted in M. If D is 0, values have no decimal point or fractional part."
                    "The maximum number of digits (M) for DECIMAL is 65. The maximum number of supported decimals (D) is 30. If D is omitted, the default is 0. If M is omitted, the default is 10."
                    "UNSIGNED, if specified, disallows negative values."
                    "All basic calculations (+, -, *, /) with DECIMAL columns are done with a precision of 65 digits.",

                    "FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]": "A small (single-precision) floating-point number. Permissible values are -3.402823466E+38 to -1.175494351E-38, 0, and 1.175494351E-38 to 3.402823466E+38.",

                    "FLOAT(p) [UNSIGNED] [ZEROFILL]": "A floating-point number. p represents the precision in bits, but MySQL uses this value only to determine whether to use FLOAT or DOUBLE for the resulting data type.",

                    "DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]": "A normal-size (double-precision) floating-point number. Permissible values are -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308."},

                    "Date and Time": {"DATE": "A date. The supported range is '1000-01-01' to '9999-12-31'. MySQL displays DATE values in 'YYYY-MM-DD' format, but permits assignment of values to DATE columns using either strings or numbers.",

                    "DATETIME[(fsp)]": "A date and time combination. The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'. MySQL displays DATETIME values in 'YYYY-MM-DD HH:MM:SS[.fraction]' format,"
                    "but permits assignment of values to DATETIME columns using either strings or numbers.An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision."
                    " A value of 0 signifies that there is no fractional part. If omitted, the default precision is 0.",

                    "TIMESTAMP[(fsp)]": "A timestamp. The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC. TIMESTAMP values are stored as the number of seconds since the epoch ('1970-01-01 00:00:00' UTC)."
                    "A TIMESTAMP cannot represent the value '1970-01-01 00:00:00' because that is equivalent to 0 seconds from the epoch and the value 0 is reserved for representing '0000-00-00 00:00:00', the “zero” TIMESTAMP value."
                    "An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value of 0 signifies that there is no fractional part. If omitted, the default precision is 0.",

                    "TIME[(fsp)]": "A time. The range is '-838:59:59.000000' to '838:59:59.000000'. MySQL displays TIME values in 'HH:MM:SS[.fraction]' format, but permits assignment of values to TIME columns using either strings or numbers."
                    "An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value of 0 signifies that there is no fractional part. If omitted, the default precision is 0.",

                    "YEAR[(4)]": "A year in four-digit format. MySQL displays YEAR values in YYYY format, but permits assignment of values to YEAR columns using either strings or numbers. Values display as 1901 to 2155, and 0000."},

                    "String Type":
                    {"CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]": "A fixed-length string that is always right-padded with spaces to the specified length when stored. M represents the column length in characters. The range of M is 0 to 255. If M is omitted, the length is 1.",


                    "VARCHAR(M) [CHARACTER SET charset_name] [COLLATE collation_name]": "A variable-length string. M represents the maximum column length in characters. The range of M is 0 to 65,535. The effective maximum length of a VARCHAR is subject to the maximum row size (65,535 bytes, which is shared among all columns) and the character set used.",

                    "BINARY[(M)]": "The BINARY type is similar to the CHAR type, but stores binary byte strings rather than nonbinary character strings. An optional length M represents the column length in bytes. If omitted, M defaults to 1.",

                    "VARBINARY(M)": "The VARBINARY type is similar to the VARCHAR type, but stores binary byte strings rather than nonbinary character strings. M represents the maximum column length in bytes.",

                    "TINYBLOB": "A BLOB column with a maximum length of 255 (28 − 1) bytes. Each TINYBLOB value is stored using a 1-byte length prefix that indicates the number of bytes in the value.",

                    "TINYTEXT [CHARACTER SET charset_name] [COLLATE collation_name]": "A TEXT column with a maximum length of 255 (28 − 1) characters. The effective maximum length is less if the value contains multibyte characters. Each TINYTEXT value is stored using a 1-byte length prefix that indicates the number of bytes in the value.",

                    "BLOB[(M)]": "A BLOB column with a maximum length of 65,535 (216 − 1) bytes. Each BLOB value is stored using a 2-byte length prefix that indicates the number of bytes in the value."
                    "An optional length M can be given for this type. If this is done, MySQL creates the column as the smallest BLOB type large enough to hold values M bytes long.",

                    "TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]": "A TEXT column with a maximum length of 65,535 (216 − 1) characters. The effective maximum length is less if the value contains multibyte characters. Each TEXT value is stored using a 2-byte length prefix that indicates the number of bytes in the value."
                    "An optional length M can be given for this type. If this is done, MySQL creates the column as the smallest TEXT type large enough to hold values M characters long.",

                    "MEDIUMBLOB": "A BLOB column with a maximum length of 16,777,215 (224 − 1) bytes. Each MEDIUMBLOB value is stored using a 3-byte length prefix that indicates the number of bytes in the value.",

                    "MEDIUMTEXT [CHARACTER SET charset_name] [COLLATE collation_name]": "A TEXT column with a maximum length of 16,777,215 (224 − 1) characters. The effective maximum length is less if the value contains multibyte characters. Each MEDIUMTEXT value is stored using a 3-byte length prefix that indicates the number of bytes in the value.",

                    "LONGBLOB": "A BLOB column with a maximum length of 4,294,967,295 or 4GB (232 − 1) bytes. The effective maximum length of LONGBLOB columns depends on the configured maximum packet size in the client/server protocol and available memory. Each LONGBLOB value is stored using a 4-byte length prefix that indicates the number of bytes in the value.",

                    "LONGTEXT [CHARACTER SET charset_name] [COLLATE collation_name]": "A TEXT column with a maximum length of 4,294,967,295 or 4GB (232 − 1) characters. The effective maximum length is less if the value contains multibyte characters. The effective maximum length of LONGTEXT columns also depends on the configured maximum packet size in the"
                    "client/server protocol and available memory. Each LONGTEXT value is stored using a 4-byte length prefix that indicates the number of bytes in the value.",

                    "ENUM('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]": "An enumeration. A string object that can have only one value, chosen from the list of values 'value1', 'value2', ..., NULL or the special '' error value. ENUM values are represented internally as integers."
                    "An ENUM column can have a maximum of 65,535 distinct elements. (The practical limit is less than 3000.) A table can have no more than 255 unique element list definitions among its ENUM and SET columns considered as a group.",

                    "SET('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]": "A set. A string object that can have zero or more values, each of which must be chosen from the list of values 'value1', 'value2', ... SET values are represented internally as integers."
                    "A SET column can have a maximum of 64 distinct members. A table can have no more than 255 unique element list definitions among its ENUM and SET columns considered as a group."},
                    }

# On retire les options inutiles
for keys, values in type_MySQL_help.items():
    for key, value in values.items():
        if " [CHARACTER SET charset_name] [COLLATE collation_name]" in key:
            key_f = key.replace(" [CHARACTER SET charset_name] [COLLATE collation_name]", "")
            type_MySQL_help[keys][key_f] = type_MySQL_help[keys][key]
            del type_MySQL_help[keys][key]


integralList = []
dateList = []
StringList = []

# Création de listes pour créé le .csv

for keys, values in type_MySQL_help.items():
    for key, value in values.items():
        if keys == "Integral":
            integralList.append([key, 0, value])
        if keys == "Date and Time":
            dateList.append([key, 0, value])
        if keys == "String Type":
            StringList.append([key, 0, value])

# formatage des commandes python.

i = 0
for items in integralList:
    if "[" in items[0]:
        f_cmd = items[0].replace("[", "{").replace("]", "}").replace("(M)", "m").replace("(M,D)", "m").replace("UNSIGNED", "u").replace("ZEROFILL", "zf")
        integralList[i][1] = f_cmd
    else:
        integralList[i][1] = integralList[i][0]
    i += 1

print(integralList)

i = 0
for items in dateList:
    if "[" in items[0]:
        f_cmd = items[0].replace("[", "{").replace("]", "}").replace("(M)", "m").replace("(M,D)", "m").replace("UNSIGNED", "u").replace("ZEROFILL", "zf")
        dateList[i][1] = f_cmd
    else:
        dateList[i][1] = dateList[i][0]
    i+=1

print(dateList)

i = 0

for items in StringList:
    if "[" in items[0] or "(" in items[0]:
        f_cmd = items[0].replace("[", "").replace("]", "").replace("(M)", "{m}").replace("(M,D)", "m").replace("UNSIGNED", "u").replace("ZEROFILL", "zf").replace("('value1','value2',...)", "")
        StringList[i][1] = f_cmd
    else:
        StringList[i][1] = StringList[i][0]
    i += 1

print(StringList)

with open("TypeMySQL_Col.csv", "w", encoding="utf-8") as file:
    file.write("Integer Type\n")
    for items in integralList:
        file.write(";".join(items)+"\n")
    file.write("Date and Time Type\n")
    for items in dateList:
        file.write(";".join(items)+"\n")
    file.write("String Type\n")
    for items in StringList:
        file.write(";".join(items)+"\n")
