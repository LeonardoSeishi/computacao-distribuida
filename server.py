# server.py
from flask import Flask, request, jsonify
import random, string

app = Flask(__name__)
urls = {}  # dicionário: código curto -> URL original

def gerar_codigo(tamanho=6):
    """Gera um código aleatório de letras e dígitos."""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=tamanho))

@app.route('/shorten', methods=['POST'])
def encurtar():
    dados = request.get_json()
    if not dados or 'url' not in dados:
        return jsonify({'error': 'URL não fornecida'}), 400

    # Garante código único
    while True:
        codigo = gerar_codigo()
        if codigo not in urls:
            break

    urls[codigo] = dados['url']
    return jsonify({'shortcode': codigo}), 201

@app.route('/resolve/<codigo>', methods=['GET'])
def resolver(codigo):
    url = urls.get(codigo)
    if url is None:
        return jsonify({'error': 'Código não encontrado'}), 404
    return jsonify({'url': url}), 200

@app.route('/<codigo>', methods=['DELETE'])
def remover(codigo):
    if codigo not in urls:
        return jsonify({'error': 'Código não encontrado'}), 404
    del urls[codigo]
    return '', 204

if __name__ == '__main__':
    # host e porta fixos, podem ser lidos do config.txt (opcional)
    app.run(host='127.0.0.1', port=5000)