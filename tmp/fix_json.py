import re
import json
import os

input_file = r"c:\Users\conan.costa\Desktop\arxextracao\protocolos.json"
output_file = r"c:\Users\conan.costa\Desktop\arxextracao\protocolos.json"

try:
    with open(input_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Extrai apenas os números usando regex
    protocolos = re.findall(r"\d+", content)
    
    # Remove duplicados (caso o usuário tenha colado algo a mais) mas mantém a ordem se possível
    # protocolos = list(dict.fromkeys(protocolos)) 

    # Cria o novo dicionário JSON
    data = {
        "protocolos": [int(p) for p in protocolos]
    }

    # Salva com indentação bonita
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"Sucesso! {len(protocolos)} protocolos foram processados e o arquivo foi corrigido.")
except Exception as e:
    print(f"Erro ao processar arquivo: {e}")
