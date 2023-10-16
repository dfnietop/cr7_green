import datetime
import random
import boto3
import os
import io
from io import BytesIO, StringIO
import uuid
import csv
from datetime import datetime

import pandas as pd
import pathlib


# from DataLake import utils
# from DataLake.extractor import Extractor


# from dotenv import load_dotenv
#
# load_dotenv("../dev.env")


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key, region_name):

        self.BucketName = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:

            response = self.client.put_object(
                ACL="private", Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            print("Error : {} ".format(e))
            return "error"

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def __repr__(self):
        return "AWS S3 Helper class "


global faker
global helper

# faker = Faker()
helper = AWSS3(
    aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
    region_name=os.getenv("DEV_REGION"),
    bucket=os.getenv("BUCKET")
)


def generate_fake_data():
    # fake = Faker()

    # Generate fake orders and customers data
    orders = []
    customers = []

    for i in range(1, 20):
        order_id = uuid.uuid4().__str__()
        customer_id = uuid.uuid4().__str__()

        order = {
            "orderid": order_id,
            "customer_id": customer_id,
            "ts": datetime.now().isoformat().__str__(),
            "order_value": random.randint(10, 1000).__str__(),
            "priority": random.choice(["LOW", "MEDIUM", "URGENT"])
        }
        orders.append(order)

        # customer = {
        #     "customer_id": customer_id,
        #     "name": fake.name(),
        #     "state": fake.state(),
        #     "city": fake.city(),
        #     "email": fake.email(),
        #     "ts": datetime.now().isoformat().__str__()
        # }
        # customers.append(customer)

    return orders, customers


def dump_csv_to_s3(data, key_prefix):
    csv_data = [list(data[0].keys())] + [list(item.values()) for item in data]

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(csv_data)

    # Convert the CSV data to bytes
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # Upload the CSV file to S3
    file_key = f'{key_prefix}/{uuid.uuid4().__str__()}.csv'
    helper.put_files(Response=csv_bytes, Key=file_key)


if __name__ == "__main__":
    chunk_size = 100000
    # orders, customers = generate_fake_data()
    # Dump orders and customers data to CSV files and upload to S3
    # dump_csv_to_s3(orders, "raw/orders")
    # dump_csv_to_s3(customers, "raw/customers")
    try:
        usecols = ['WID_PRODUCTO','CODIGO_SUCURSAL', 'DESCRIPCION_SUCURSAL', 'CODIGO_ARTICULO',
                   'DESCRIPCION_ARTICULO', 'ESTADOS', 'UNIDAD_DE_MEDIDA_MINIMA',
                   'CANTIDAD_UNIDAD_MINIMA', 'UNIDAD_DE_MEDIDA_LOGISTICA',
                   'CANTIDAD_UNIDAD_LOGISTICA', 'COSTO_UNITARIO_UNIDAD_MINIMA',
                   'LOTE_PROVEEDOR', 'LOTE_SAP', 'FECHA_VENCIMIENTO', 'SISTEMA_ORIGEN', 'FECHA_ACTUALIZACION'
                   ]

        ejemplo_dir = 'C:\\Users\\daniel.nieto_bluetab\\Documents\\Proyectos\\Cruz Verde\\STOCK CRUZ VERDE MVP'
        directorio = pathlib.Path(ejemplo_dir)

        for fichero in directorio.iterdir():
            print(fichero.name)
            if 'txt' in fichero.name:
                df = pd.read_csv(f'{ejemplo_dir}\\{fichero.name}',encoding='latin1', sep='|', engine='python')

                df['WID_PRODUCTO'] =df['WID_PRODUCTO'].astype(int)
                df['CODIGO_SUCURSAL'] =df['CODIGO_SUCURSAL'].astype(int)
                df['DESCRIPCION_SUCURSAL'] = df['DESCRIPCION_SUCURSAL'].astype(str)
                df['CODIGO_ARTICULO'] = df['CODIGO_ARTICULO'].astype(str)
                df['DESCRIPCION_ARTICULO'] = df['DESCRIPCION_ARTICULO'].astype(str)
                df['ESTADOS'] = df['ESTADOS'].astype(str)
                df['UNIDAD_DE_MEDIDA_LOGISTICA'] = df['UNIDAD_DE_MEDIDA_LOGISTICA'].astype(str)
                df['CANTIDAD_UNIDAD_LOGISTICA'] = df['CANTIDAD_UNIDAD_LOGISTICA'].astype(str).apply(lambda x: x.replace(',', '.')).astype(float)
                df['COSTO_UNITARIO_UNIDAD_MINIMA'] = df['COSTO_UNITARIO_UNIDAD_MINIMA'].astype(str)
                df['LOTE_PROVEEDOR'] = df['LOTE_PROVEEDOR'].astype(str)
                df['LOTE_SAP'] = df['LOTE_SAP'].astype(str)
                df['SISTEMA_ORIGEN'] = df['SISTEMA_ORIGEN'].astype(str)
                df['FECHA_VENCIMIENTO'] = df['FECHA_VENCIMIENTO'].astype(str)
                df['FECHA_ACTUALIZACION'] = df['FECHA_ACTUALIZACION'].astype(str)
                df['UNIDAD_DE_MEDIDA_MINIMA'] = df['UNIDAD_DE_MEDIDA_MINIMA'].astype(str)
                # df['CANTIDAD_UNIDAD_MINIMA'] = df['CANTIDAD_UNIDAD_MINIMA'].astype(float)
                df['CANTIDAD_UNIDAD_MINIMA'] = df['CANTIDAD_UNIDAD_MINIMA'].astype(str).apply(lambda x: x.replace(',', '.')).astype(float)

                df = df[usecols]

                for i in range(0, len(df), chunk_size):
                    slc = df.iloc[i: i + chunk_size]
                    chunk = int(i / chunk_size)

                    out_buffer = BytesIO()
                    slc.to_parquet(out_buffer, index=False)

                    key_prefix = "raw-dev"
                    file_key = f'{key_prefix}/{uuid.uuid4().__str__()}.parquet'
                    helper.put_files(Response=out_buffer.getvalue(), Key=file_key)
                    print(f'upload chunk: {file_key}')
    except Exception as e:
        raise

# extractor= Extractor(utils.BOPOS)
