import pandas as pd
import json
from DE_Machine_Data_Framework.Machine_Data_Pipeline import Machine_Data_Pipeline


def get_folders(pipeline):
    blobs = pipeline.get_files(file_format="CSV")
    for file in blobs:
        print(file.name)
    folders = list(blobs.prefixes)
    return folders


def read_multiple_csvs(pipeline, folder):
    df = pd.DataFrame()
    blobs = list(pipeline.get_files(prefix=folder))

    for blob in blobs:
        file = pipeline.read_file_as_dataframe(blob, index_col=False)
        df = pd.concat([df, file])

    return df


def process_csv(df, machine):
    # Cleaning up the df
    df["DT_No"] = f"DT-{machine}"
    tagnames = json.load(
        open(f"/home/dataengineer/shendra_draw_tower/tags/{machine}_tags.json")
    )
    df["TagIndex"] = df["TagIndex"].astype(str).replace(tagnames)
    print(df.head())

    # Making sure the datatypes are correct
    print("No of rows in main table :", df.shape[0])
    return df


def process_pivot_csv(df):
    print("Generating pivot table...")
    pivot_df = df.pivot_table(
        index=["Date", "Time", "Millitm", "DT_No"],
        columns="TagIndex",
        values="Value",
    ).reset_index()
    pivot_df.columns = map(str.upper, pivot_df.columns)
    print("No of rows in pivot table :", pivot_df.shape[0])

    return pivot_df


if __name__ == "__main__":

    plant = "Shendra_Draw"
    process = "Draw_Tower"
    # dataset = "Shendra_Draw_Tower"
    # machines = {
    #     "gen1": ["DT-42", "DT-43", "DT-44", "DT-45"],
    #     "gen2": ["DT-47", "DT-48", "DT-51"],
    # }
    dataset = "data_engineering"
    machines = {"gen1": ["DT-44"]}

    for gen in machines.keys():
        for machine in machines[gen]:
            try:
                pipeline = Machine_Data_Pipeline(
                    plant, process, machine, dataset, f"{gen}_new"
                )
                print(f"___________DT-{machine}_____________\n\n")

                folders = get_folders(pipeline)
                if not folders:
                    pipeline.files_not_found()
                    continue

                for folder in folders:
                    foldername = folder.rstrip("/").split("/")[-1]
                    try:
                        print(folder)
                        df = read_multiple_csvs(pipeline, folder)

                        # gen{x}_main table
                        # pipeline.set_table("f{gen}_new")
                        df = process_csv(df, machine)
                        pipeline.upload_to_bq()
                        pipeline.update_audit_table(
                            foldername, records_per_load=df.size, job_status="Success"
                        )

                        # gen{x}_pivot_table
                        pipeline.set_table("f{gen}_pivot")
                        pivot_df = process_pivot_csv(df)
                        pipeline.upload_to_bq(pivot_df)

                        pipeline.move_to_archive(folder)
                        pipeline.update_audit_table(
                            folder, records_per_load=pivot_df.size, job_status="Success"
                        )

                    except Exception as e:
                        print(e)
                        pipeline.move_to_error(folder)
                        pipeline.add_error(foldername, e)
                        pipeline.update_audit_table(
                            foldername, records_per_load=0, job_status="Failed"
                        )
                        pass

                if pipeline.has_errors():
                    pipeline.send_email()

            except Exception as e:
                print(e)
                pipeline.send_email(e)
                pass
