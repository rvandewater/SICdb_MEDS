import struct
from datetime import timedelta

import polars as pl


def is_hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def unpack_waveform(df: pl.LazyFrame, schema: dict) -> pl.LazyFrame:
    def process_batch(batch):
        offsets = []
        ids = []
        vals = []
        patient_ids = []
        case_ids = []
        reference_values = []
        reference_units = []

        for row in batch.iter_rows(named=True):
            t = row["Offset"]
            rawdata = row["rawdata"]
            if rawdata is None or len(rawdata) < 3 or not is_hex(rawdata[2:]):
                continue
            try:
                data = bytes.fromhex(rawdata[2:])
            except ValueError:
                continue
            n = row["id"]
            for i in range(int(len(data) / 4)):
                if data[i * 4 : i * 4 + 4] == b"\x00\x00\x00\x00":
                    continue
                n += 1
                offsets.append(t + timedelta(minutes=i))
                ids.append(n)
                vals.append(struct.unpack("<f", data[i * 4 : i * 4 + 4])[0])
                patient_ids.append(row["PatientID"])
                case_ids.append(row["CaseID"])
                reference_values.append(row["ReferenceValue"])
                reference_units.append(row["ReferenceUnit"])

        if not offsets:
            return pl.DataFrame(schema=schema)
        reference_units = reference_units if reference_units else [""] * len(ids)
        return pl.DataFrame(
            {
                "PatientID": patient_ids,
                "CaseID": case_ids,
                "Offset": offsets,
                "id": ids,
                "Val": vals,
                "ReferenceValue": reference_values,
                "ReferenceUnit": reference_units,
            }
        )

    processed_df = df.map_batches(
        process_batch,
        schema=schema,
        streamable=True,
    )
    return processed_df


def unpack_waveform_dict(df: pl.LazyFrame) -> pl.LazyFrame:
    def process_batch(batch):
        rows = []
        for row in batch.iter_rows(named=True):
            t = row["Offset"]
            if row["rawdata"] is None:
                continue
            try:
                data = bytes.fromhex(
                    row["rawdata"][2:]
                )  # deserialize hex string to bytes
            except ValueError:
                continue
            n = row["id"]
            for i in range(int(len(data) / 4)):
                if (
                    data[i * 4] == 0
                    and data[i * 4 + 1] == 0
                    and data[i * 4 + 2] == 0
                    and data[i * 4 + 3] == 0
                ):
                    continue  # no null values
                n += 1  # new primary key
                newrow = row.copy()
                newrow["id"] = n  # primary key
                newrow["Val"] = struct.unpack("<f", data[i * 4 : i * 4 + 4])[
                    0
                ]  # bytes to float
                newrow["Offset"] = t + timedelta(
                    minutes=i
                )  # pl.duration(minutes=i) #i * 60 #new offset
                # print(newrow)
                rows.append(newrow)
            # print(rows[0])
            print(f"Processed batch, length {batch.height}")
            if not rows:
                return pl.DataFrame(
                    schema={
                        "PatientID": pl.Int64,
                        "CaseID": pl.Int64,
                        "Offset": pl.Datetime("us", None),
                        "id": pl.Int64,
                        "Val": pl.Float64,
                        "ReferenceValue": pl.String,
                        "ReferenceUnit": pl.String,
                    }
                )
            return pl.DataFrame(rows).select(pl.exclude("rawdata", "cnt"))

    # Apply the processing function to each batch
    processed_df = df.map_batches(
        process_batch,
        schema={
            "PatientID": pl.Int64,
            "CaseID": pl.Int64,
            "Offset": pl.Datetime("us", None),
            "id": pl.Int64,
            "Val": pl.Float64,
            "ReferenceValue": pl.String,
            "ReferenceUnit": pl.String,
        },
        streamable=True,
    )
    return processed_df


def unpack_waveform_para(df: pl.LazyFrame) -> pl.LazyFrame:
    def process_batch(batch):
        # Filter out invalid rows first
        valid_rows = batch.filter(
            (pl.col("rawdata").is_not_null())
            &
            # (pl.col("rawdata").str.len_bytes() >= 3) &
            (
                pl.col("rawdata").map_elements(
                    lambda x: is_hex(x[2:]), return_dtype=pl.Boolean
                )
            )
        )
        index_rawdata = batch.get_column_index("rawdata")

        # Convert hex to bytes and unpack
        def unpack_data(row):
            try:
                data = bytes.fromhex(row[index_rawdata][2:])
                return tuple(
                    struct.unpack("<f", data[i * 4 : i * 4 + 4])[0]
                    for i in range(len(data) // 4)
                )
            except ValueError:
                return tuple()

        unpacked_data = valid_rows.map_rows(
            unpack_data, return_dtype=pl.List(pl.Float64)
        )

        print(unpacked_data)
        # Explode the unpacked data into separate rows
        exploded = valid_rows.unpacked_data.explode("unpacked_data")

        # Generate new ids and offsets
        exploded = exploded.with_columns(
            [
                (
                    pl.col("Offset")
                    + pl.arange(0, pl.count(), 1) * timedelta(minutes=1)
                ).alias("Offset"),
                (pl.col("id") + pl.arange(1, pl.count() + 1, 1)).alias("id"),
                pl.col("unpacked_data").alias("Val"),
            ]
        )

        # Select the required columns
        result = exploded.select(
            [
                "PatientID",
                "CaseID",
                "Offset",
                "id",
                "Val",
                "ReferenceValue",
                "ReferenceUnit",
            ]
        )

        return result

    processed_df = df.map_batches(
        process_batch,
        schema={
            "PatientID": pl.Int64,
            "CaseID": pl.Int64,
            "Offset": pl.Datetime("us", None),
            "id": pl.Int64,
            "Val": pl.Float64,
            "ReferenceValue": pl.String,
            "ReferenceUnit": pl.String,
        },
        streamable=True,
    )
    return processed_df
