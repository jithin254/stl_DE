from DE_Machine_Data_Framework.Machine_Data_Pipeline import Machine_Data_Pipeline
from datetime import datetime


def trigger_pipeline(plant, process, table, machines, process_csv):
    print("Starting...")
    job_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for machine in machines:
        try:
            pipeline = Machine_Data_Pipeline(plant, process, machine, table)
            print(f"{machine}")
            files = list(pipeline.get_files())
            print(files)
            if files:
                for file in files:
                    print(file.name)
                    if not file.name.endswith("/"):
                        try:
                            print(file.name)
                            job_start_time = datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )

                            df = pipeline.read_file_as_dataframe(file)
                            df = process_csv(df)
                            pipeline.upload_to_bq(df)
                            pipeline.move_to_archive(file)

                            records_per_load = df.size[0]
                            job_status = "success"
                                                    
                            pipeline.update_audit_table(
                                file.name,
                                records_per_load,
                                job_status,
                                job_start_time
                            )

                        except Exception as e:
                            pipeline.errors[file.name] = e
                            pipeline.move_to_error(file)
                            records_per_load = 0
                            job_status = "failed"

                            pipeline.update_audit_table(
                                file.name,
                                records_per_load,
                                job_status,
                                job_start_time
                            )
                            pass



                if pipeline.errors:
                    pipeline.send_email()
            else:
                e = f"Files Not Found for {plant}_{process} machine {machine}"
                print(e)
                pipeline.send_email(e)

        except Exception as e:
            print(e)
            pipeline.send_email(e)
            pass
