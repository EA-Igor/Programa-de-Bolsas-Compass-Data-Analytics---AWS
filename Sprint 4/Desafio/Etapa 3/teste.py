import hashlib

def gerar_hash_sha1(texto):
    #Função que gera o hash SHA-1 de uma string.

    sha1 = hashlib.sha1()
    sha1.update(texto.encode('utf-8'))
    return sha1.hexdigest()

def main():
    while True:
        texto = input("Digite uma string para gerar o hash SHA-1 (ou 'sair' para terminar): ")
        
        if texto.lower() == 'sair':
            print("Encerrando o programa.")
            break
        
        hash_sha1 = gerar_hash_sha1(texto)
        
        # Imprime o hash em tela
        print(f"Hash SHA-1: {hash_sha1}\n")

if __name__ == "__main__":
    main()
