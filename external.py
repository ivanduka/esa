from wand.image import Image
from sqlalchemy import text, create_engine
from io import StringIO
import sys
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
import camelot
from uuid import uuid4
import pandas as pd
import traceback
import io
import json


def extract_csv(args):
    buf = StringIO()
    with redirect_stdout(buf), redirect_stderr(buf):
        pdf_id, page, engine_string, pdf_files_folder_string, csv_tables_folder_string = args
        pdf_files_folder = Path(pdf_files_folder_string)
        csv_tables_folder = Path(csv_tables_folder_string)
        engine = create_engine(engine_string)

        def save_tables(tables, method):
            print(f"{pdf_id} on page {page}: found {len(tables)} tables with {method}")
            for index, table in enumerate(tables):
                table_number = index + 1
                csv_id = f"{pdf_id}_{page}_{method}_{table_number}"
                csv_file_name = f"{csv_id}.csv"
                csv_full_path = str(csv_tables_folder.joinpath(csv_file_name))
                csvRows, csvColumns = table.shape
                accuracy = table.accuracy
                whitespace = table.whitespace
                order = table.order
                top_row_json = json.dumps(table.df.iloc[0].tolist())
                s = io.StringIO()
                table.to_csv(s, index=False, header=False, encoding="utf-8-sig")
                csv_text = s.getvalue()
                # print(f"csvId: {csv_id}")
                # print(f"csvFileName: {csv_file_name}")
                # print(f"pdfId: {pdf_id}")
                # print(f"page: {page}")
                # print(f"tableNumber: {table_number}")
                # print(f"csvPath: {csv_full_path}")
                # print(f"topRowJson: {top_row_json}")
                # print(f"rows: {rows}")
                # print(f"columns: {columns}")
                # print(f"method: {method}")
                # print(f"accuracy: {accuracy}")
                # print(f"whitespace: {whitespace}")
                table.to_csv(csv_full_path, index=False, header=False, encoding="utf-8-sig")

                with engine.connect() as conn:
                    statement = text(
                        "INSERT INTO esa.csvs (csvId, csvFileName, csvFullPath, pdfId, page, tableNumber," +
                        "topRowJson, csvRows, csvColumns, method, accuracy, whitespace, csvText) " +
                        "VALUE (:csvId, :csvFileName, :csvFullPath, :pdfId, :page, :tableNumber, " +
                        ":topRowJson, :csvRows, :csvColumns, :method, :accuracy, :whitespace, :csvText);")
                    result = conn.execute(statement, {"csvId": csv_id, "csvFileName": csv_file_name,
                                                      "csvFullPath": csv_full_path, "pdfId": pdf_id,
                                                      "page": page, "tableNumber": table_number,
                                                      "topRowJson": top_row_json, "csvRows": csvRows,
                                                      "csvColumns": csvColumns, "method": method,
                                                      "accuracy": accuracy, "whitespace": whitespace,
                                                      "csvText": csv_text})
                print(f"{pdf_id} on page {page}: inserted {result.rowcount} rows for CSV {csv_id}")

        try:
            pdf_file_path = pdf_files_folder.joinpath(f"{pdf_id}.pdf")
            tables = camelot.read_pdf(str(pdf_file_path), pages=str(page), strip_text='\n',
                                      line_scale=40, flag_size=True, copy_text=['v'],)
            save_tables(tables, "lattice-v")
            print(f"{pdf_id} on page {page}: done successfully.")
        except Exception as e:
            print(f'Error processing {pdf_id} on page {page}:')
            print(e)
            traceback.print_tb(e.__traceback__)
        finally:
            return buf.getvalue()
