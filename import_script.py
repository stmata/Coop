import os
import json
import boto3
import pymongo
from dotenv import load_dotenv

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = pymongo.MongoClient(mongo_uri)
db = client["COOP"]

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
bucket_name = os.getenv("AWS_BUCKET_NAME")
print(f"Bucket name lu depuis .env : '{bucket_name}'") 

json_file = "TuristGuide.json"
with open(json_file, "r", encoding="utf-8") as f:
    data = json.load(f)

categories_mapping = {
    "monuments": [
        "Basílica de Nossa Senhora de Nazaré",
        "Forte do Presépio / Museu Forte do Presépio",
        "Catedral Metropolitana de Belém",
        "Palácio Antonio Lemos",
        "Palácio Lauro Sodré / Museu Histórico do Pará",
        "Complexo Feliz Lusitânia",
        "Centro Histórico de Belém",
        "Praça do Relógio",
        "Praça da República de Belém"
    ],
    "musees": [
        "Museu Paraense Emílio Goeldi",
        "Museu de Arte de Belém",
        "Forte do Presépio / Museu Forte do Presépio",
        "Palácio Lauro Sodré / Museu Histórico do Pará"
    ],
    "lieux_culturels": [
        "Teatro da Paz",
        "Casa das Onze Janelas",
        "Hangar - Centro de Convenções e Feiras da Amazônia",
        "Estação das Docas"
    ],
    "parcs": [
        "Mangal das Garças",
        "Parque da Residência",
        "Parque Estadual do Utinga",
        "Bosque Rodrigues Alves - Jardim Botânico da Amazônia",
        "Praça Batista Campos"
    ],
    "plages": [
        "Ilha de Cotijuba"
    ],
    "marches": [
        "Mercado Ver-o-Peso"
    ],
    "restaurants": [
    ]
}

def determine_category(item):
    name_pt = item.get("nome", {}).get("pt", "").strip()
    for cat, names in categories_mapping.items():
        if name_pt in names:
            return cat
    return "divers"

images_base_path = "images"

for i, item in enumerate(data, start=1):
    doc = {
        "name": item.get("nome", {}),
        "description": item.get("descricao", {}),
        "address": item.get("endereco", {}),
        "history": item.get("historia", {}),
        "schedules": item.get("horarios", {}),
        "ticket_price": item.get("taxa_entrada", {}),
        "theme": item.get("tematica", {}),
        "popularity": item.get("popularidade", {}),
        "danger_level": item.get("periculosidade", {}),
        "url": item.get("url", {}),
        "images": []
    }

    category = determine_category(item)
    collection = db[category]

    folder_name = None
    for d in os.listdir(images_base_path):
        if d.startswith(f"{i}_"):
            folder_name = d
            break

    if folder_name:
        folder_path = os.path.join(images_base_path, folder_name)
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                s3_key = f"turist_guide/{folder_name}/{file_name}"
                s3.upload_file(file_path, bucket_name, s3_key)
                image_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
                doc["images"].append({
                    "filename": file_name,
                    "url": image_url
                })
    else:
        print(f"Aucun dossier d'image trouvé pour l'entrée {i} ({doc.get('name', {}).get('pt', 'Inconnu')})")

    result = collection.insert_one(doc)
    print(f"[{category.upper()}] Document inséré : {doc['name'].get('pt', 'Inconnu')} (_id={result.inserted_id})")

for cat in categories_mapping.keys():
    if cat not in db.list_collection_names():
        db[cat].insert_one({"_dummy": True})
        db[cat].delete_one({"_dummy": True})
        print(f"Collection '{cat}' créée vide.")

print("=== IMPORTATION TERMINÉE ===")
