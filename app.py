from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
import os
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import io
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB

# Configurações do S3
S3_BUCKET = os.getenv('S3_BUCKET')
S3_REGION = os.getenv('S3_REGION')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')

# Função para conectar ao banco de dados
def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

# Função para conectar ao S3
def create_s3_client():
    try:
        s3_client = boto3.client('s3', 
                                 region_name=S3_REGION,
                                 aws_access_key_id=S3_ACCESS_KEY,
                                 aws_secret_access_key=S3_SECRET_KEY)
        return s3_client
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error connecting to S3: {e}")
        return None

# Rota para upload de arquivo
@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify(error='No file part'), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify(error='No selected file'), 400

        # Verificar se o arquivo é do tipo text/csv
        if file.mimetype != 'text/csv':
            # Se não for, converter para text/csv
            file.stream.seek(0)
            file = io.BytesIO(file.stream.read().decode('utf-8').encode('utf-8-sig'))

        filename = secure_filename(file.filename)
        s3_client = create_s3_client()
        if not s3_client:
            return jsonify(error='Failed to connect to S3'), 500

        try:
            s3_client.upload_fileobj(file, S3_BUCKET, filename)
        except Exception as e:
            print(f"Failed to upload file to S3: {e}")
            return jsonify(error='Failed to upload file'), 500

        file_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{filename}"

        # Salvar detalhes do arquivo no banco de dados
        connection = create_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("INSERT INTO files (filename, file_url) VALUES (%s, %s)",
                               (filename, file_url))
                connection.commit()
            except Error as e:
                print(f"Failed to insert record into MySQL table: {e}")
                return jsonify(error='Failed to save file details in database'), 500
            finally:
                cursor.close()
                connection.close()

        return jsonify(fileUrl=file_url), 200
    except Exception as e:
        print(f"Unexpected error during file upload: {e}")
        return jsonify(error='Internal Server Error'), 500

# Rota para obter arquivo carregado com tipo condicional
@app.route('/uploads/<filename>', methods=['GET'])
def uploaded_file(filename):
    try:
        # Buscar a URL do arquivo no banco de dados
        connection = create_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT file_url FROM files WHERE filename = %s", (filename,))
                row = cursor.fetchone()
                if row:
                    file_url = row[0]
                    # Verifica se o tipo do arquivo é CSV
                    if filename.endswith('.csv'):
                        # Retorna a URL do arquivo com o tipo /csv
                        return jsonify(fileUrl=file_url, type='text/csv'), 200
                    else:
                        # Retorna a URL do arquivo sem especificar o tipo
                        return jsonify(fileUrl=file_url), 200
                else:
                    print(f"File not found in database for filename: {filename}")
                    return jsonify(error='File not found'), 404
            except Error as e:
                print(f"Failed to fetch file from MySQL table: {e}")
                return jsonify(error='Failed to fetch file from database'), 500
            finally:
                cursor.close()
                connection.close()
    except Exception as e:
        print(f"Unexpected error during file fetch: {e}")
        return jsonify(error='Internal Server Error'), 500

if __name__ == '__main__':
    app.run(debug=True)
