"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import os
import zipfile
from datetime import datetime

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    """
    Limpia y transforma los datos de una campaña bancaria según las especificaciones,
    generando tres archivos de salida: client.csv, campaign.csv y economics.csv.
    """

    def load_zip_data(input_folder):
        """Carga los datos de los archivos ZIP en la carpeta de entrada y devuelve una lista de DataFrames."""
        zip_files = [f for f in os.listdir(input_folder) if f.endswith('.zip')]
        dataframes = []

        for zip_file in zip_files:
            zip_path = os.path.join(input_folder, zip_file)
            with zipfile.ZipFile(zip_path, 'r') as z:
                for csv_file in z.namelist():
                    with z.open(csv_file) as file:
                        df = pd.read_csv(file)
                        dataframes.append(df)

        return dataframes

    def process_client_data(df):
        """Procesa y limpia los datos para el archivo client.csv."""
        client_df = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
        client_df["job"] = client_df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
        client_df["education"] = client_df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
        client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
        client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
        return client_df

    def process_campaign_data(df):
        """Procesa y limpia los datos para el archivo campaign.csv."""
        campaign_df = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]].copy()
        campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
        campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
        campaign_df["last_contact_date"] = campaign_df.apply(lambda row: datetime.strptime(f"2022-{row['month']}-{row['day']}", "%Y-%b-%d").strftime("%Y-%m-%d"), axis=1)
        campaign_df.drop(columns=["day", "month"], inplace=True)
        return campaign_df

    def process_economics_data(df):
        """Procesa y limpia los datos para el archivo economics.csv."""
        economics_df = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
        return economics_df

    def save_processed_data(client_data, campaign_data, economics_data, output_folder):
        """Guarda los DataFrames procesados en archivos CSV."""
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        client_data.to_csv(os.path.join(output_folder, "client.csv"), index=False)
        campaign_data.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
        economics_data.to_csv(os.path.join(output_folder, "economics.csv"), index=False)

    def main(input_folder, output_folder):
        """Ejecuta el procesamiento completo de los datos."""
        dataframes = load_zip_data(input_folder)

        client_data = []
        campaign_data = []
        economics_data = []

        for df in dataframes:
            client_data.append(process_client_data(df))
            campaign_data.append(process_campaign_data(df))
            economics_data.append(process_economics_data(df))

        client_data = pd.concat(client_data)
        campaign_data = pd.concat(campaign_data)
        economics_data = pd.concat(economics_data)

        save_processed_data(client_data, campaign_data, economics_data, output_folder)

    if __name__ == "__main__":
        main(
            input_folder="files/input/",
            output_folder="files/output/",
        )
print(clean_campaign_data())