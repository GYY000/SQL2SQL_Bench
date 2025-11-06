# Data preparation

## DataBase:

combine the human resource and order entry

## Schema Structure

```json5
{
    "table": "FacebookUsers",
    "primary_key": [
        "id"
    ],
    "cols": [
        {
            "col_name": "id",
            "type": {
                "type_name": "INT",
                "format": "MM-DD",
                // used for date-relevant Type
                "values": [
                    "M",
                    "F"
                ]
                // used for Enum Type
            },
            "attribute": [
                "NOT NULL",
                "DEFAULT NULL"
            ]
            // sevaral attributes for specific type
        }
    ],
    "foreign_key": [
        {
            "col": "account_mgr_id",
            "ref_table": "employees",
            "ref_col": "employee_id"
        }
    ],
    "index": [
        [
            "cust_last_name",
            "cust_first_name"
        ]
    ]
}
```