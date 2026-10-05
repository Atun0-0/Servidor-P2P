import socket # Para criar os sockets
import threading # Para o programa fazer várias coisas simultaneamente
import os # Para trabalhar com arquivos e diretórios
import time # Para fazer o programa esperar
import json # Para ler o config.json
import sys # Para o modo painel
from collections import deque # Guarda os últimos eventos mostrados no painel

# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Abre o arquivo config.json no modo leitura
with open(os.environ.get("CONFIG", "config.json"), "r") as arquivo:
    # Converte o JSON para um dicionário python
    config = json.load(arquivo)

PEER_ID = config["peer_id"] # Pega o ID deste peer
HOST = config["host"] # IP que o peer vai ficar escutando
PORT = config["port"] # Porta UDP que pertence a este peer
PEERS = config["peers"] # Lista com os outros peers conhecidos
BIND = config.get("bind", HOST) # IP onde o socket escuta (0.0.0.0 = todas as interfaces, usado em container/VM)

PASTA = "tmp" # Pasta que vai ser sincronizada
CHUNK_SIZE = 1024

arquivos_recebidos_rede = set() # Guarda os arquivos que foram recebidos pela rede

arquivos_removidos_rede = set() # Guarda os arquivos que foram removidos pela rede

# ============================================================
# ESTADO DA REDE (usado pelo painel)
# ============================================================

INTERVALO_HEARTBEAT = 2 # Segundos entre cada "estou vivo" enviado aos outros peers
TIMEOUT_PEER = 6 # Sem heartbeat por esse tempo = peer inativo

# Para cada peer: quando foi o último heartbeat e quais arquivos ele disse que tem
estado_peers = {p["id"]: {"ultimo": 0, "arquivos": set()} for p in PEERS}

# Pedidos de arquivo enviados que ainda não começaram a chegar: nome -> dados do pedido
pedidos_pendentes = {}

PAUSA_REPETIR = 2.5 # Segundos sem receber nada antes de pedir de novo
MAX_TENTATIVAS = 10 # Quantas vezes repete o pedido de um arquivo que não começou a chegar
TIMEOUT_TRANSFERENCIA = 60 # Depois disso, uma transferência incompleta é considerada abandonada

falhas_dns = {} # Peers que não responderam ao envio há pouco tempo (container que ainda não subiu)

# Modo painel: python peer.py painel
MODO_PAINEL = "painel" in sys.argv[1:]

eventos = deque(maxlen=10) # Últimos eventos (o que antes ia para o print)
saida_original = sys.stdout


class CapturaLog:

    # No modo painel os print() do programa viram "últimos eventos" em vez de poluir a tela
    def write(self, texto):
        for linha in texto.splitlines():
            if linha.strip():
                eventos.append(time.strftime("%H:%M:%S") + " " + linha)

    def flush(self):
        pass


if MODO_PAINEL:
    sys.stdout = CapturaLog()

# Cria a pasta tmp caso ela não exista
os.makedirs(PASTA, exist_ok=True)


# ============================================================
# SOCKET UDP
# ============================================================

# Cria um socket
sock = socket.socket(
    socket.AF_INET, # IPv4
    socket.SOCK_DGRAM # UDP
)

# Associa o socket ao IP e a porta que foram definidos antes. Agora o peer vai ficar escutando nessa porta
sock.bind((BIND, PORT))

print(f"Peer {PEER_ID} iniciado em {HOST}:{PORT}")


# ============================================================
# ENVIO DE MENSAGENS
# ============================================================

def enviar_mensagem(peer, mensagem):

    # Monta o endereço do peer que irá receber a mensagem
    endereco = (peer["host"], peer["port"])

    # Converte a mensagem de texto para bytes
    dados = mensagem.encode()

    # Peer que falhou há pouco tempo: não tenta de novo agora (evita travar esperando o DNS)
    if time.time() - falhas_dns.get(peer["id"], 0) < 5:
        return

    # Envia a mensagem via UDP
    try:
        sock.sendto(dados, endereco)
    except OSError:
        # Peer ainda não existe/não resolve (ex.: container que não subiu)
        falhas_dns[peer["id"]] = time.time()
        return

    if not mensagem.startswith("HEARTBEAT"):
        print(f"[ENVIO] {peer['id']} <- {mensagem}")


def enviar_para_todos(mensagem):
    
    # Percorre todos os peers e envia a mesma mensagem para cada um
    for peer in PEERS:
        enviar_mensagem(peer, mensagem)


def enviar_varias_vezes(mensagem):

    # Para avisos importantes (ANNOUNCE e REMOVE): o UDP pode perder datagramas,
    # então a mensagem é enviada 5 vezes (em outra thread, para não travar o monitor)
    def repetir():

        for _ in range(5):

            enviar_para_todos(mensagem)

            time.sleep(0.3)

    threading.Thread(target=repetir, daemon=True).start()


# ============================================================
# MONITORAMENTO DO TMP
# ============================================================

def listar_arquivos():

    # Retorna os nomes dos arquivos presente na pasta tmp
    return set(os.listdir(PASTA))


def monitorar_pasta():

    # Salva quais arquivos existem no início do monitoramento
    arquivos_anteriores = listar_arquivos()

    # Arquivos recebidos da rede que sumiram da pasta antes de o monitor chegar a vê-los
    suspeitos = set()

    # Fica monitorando infinitamente
    while True:

        time.sleep(2) # Espera 2 seg para verificar novamente

        arquivos_atuais = listar_arquivos()

        # Arquivos adicionados
        adicionados = arquivos_atuais - arquivos_anteriores

        # Arquivos removidos
        removidos = arquivos_anteriores - arquivos_atuais

        # Alerta arquivos novos
        for arquivo in adicionados:

            # Verifica se o arquivo foi recebido pela rede
            if arquivo in arquivos_recebidos_rede:

                # Remove da lista porque já tratamos esse arquivo
                arquivos_recebidos_rede.remove(arquivo)

                continue

            print(f"[NOVO ARQUIVO] {arquivo}")

            mensagem = f"ANNOUNCE|{arquivo}"

            enviar_varias_vezes(mensagem)

        # Alerta arquivos removidos
        for arquivo in removidos:

            # Verifica se o arquivo foi removido pela rede
            if arquivo in arquivos_removidos_rede:

                # Remove da lista porque já tratamos essa remoção
                arquivos_removidos_rede.remove(arquivo)

                continue

            print(f"[ARQUIVO REMOVIDO] {arquivo}")

            mensagem = f"REMOVE|{arquivo}"

            enviar_varias_vezes(mensagem)

        # Arquivo que chegou pela rede e foi apagado antes de o monitor vê-lo na pasta:
        # para o resto da rede isso também é uma remoção feita aqui.
        # (Só confirma na 2ª verificação seguida, para não confundir com um
        # arquivo que está sendo gravado nesse instante.)
        sumindo = {
            a for a in list(arquivos_recebidos_rede)
            if a not in arquivos_atuais
            and not os.path.exists(os.path.join(PASTA, a))
        }

        for arquivo in sumindo & suspeitos:

            arquivos_recebidos_rede.discard(arquivo)

            print(f"[ARQUIVO REMOVIDO] {arquivo}")

            enviar_varias_vezes(f"REMOVE|{arquivo}")

        suspeitos = sumindo - suspeitos

        # Atualiza para a próxima verificação com os arquivos atuais
        arquivos_anteriores = arquivos_atuais


# ============================================================
# SERVIDOR UDP
# ============================================================

def servidor():

    while True:

        try:

            dados, endereco = sock.recvfrom(65535)

        except ConnectionResetError:

            print("[ERRO] Peer remoto não está disponível.")

            continue

        if dados.startswith(b"DATA|"):

            receber_dados(
                dados,
                endereco
            )

        else:

            mensagem = dados.decode()

            if not mensagem.startswith("HEARTBEAT"):

                print(
                    f"[RECEBIDO] {endereco}: {mensagem}"
                )

            processar_mensagem(
                mensagem,
                endereco
            )


# ============================================================
# PEDIDO DE ARQUIVO
# ============================================================

def pedir_arquivo(nome_arquivo, endereco):

    caminho = os.path.join(
        PASTA,
        nome_arquivo
    )

    # Já temos o arquivo
    if os.path.exists(caminho):
        return

    # Já pedimos e estamos esperando (a repetição cuida de pedir de novo se precisar)
    if nome_arquivo in pedidos_pendentes:
        return

    # O arquivo já está chegando
    if nome_arquivo in arquivos_recebidos:

        if time.time() - arquivos_recebidos[nome_arquivo]["inicio"] <= TIMEOUT_TRANSFERENCIA:
            return

        # Transferência antiga que nunca terminou: descarta e pede do zero
        del arquivos_recebidos[nome_arquivo]

    pedidos_pendentes[nome_arquivo] = {
        "endereco": endereco,
        "ultimo": time.time(),
        "tentativas": 1
    }

    sock.sendto(
        f"GET|{nome_arquivo}".encode(),
        endereco
    )


# ============================================================
# PROCESSAMENTO DAS MENSAGENS
# ============================================================

def processar_mensagem(mensagem, endereco):

    # Divide a mensagem
    partes = mensagem.split("|")

    tipo = partes[0]


    # --------------------------------------------------------
    # ANNOUNCE
    # --------------------------------------------------------

    if tipo == "ANNOUNCE":

        nome_arquivo = partes[1]

        print(
            f"Peer anunciou o arquivo: {nome_arquivo}"
        )

        caminho = os.path.join(
            PASTA,
            nome_arquivo
        )

        # Se ainda não temos o arquivo, pedimos para quem anunciou
        # (pedir_arquivo ignora se já temos, se já pedimos ou se já está chegando)
        pedir_arquivo(nome_arquivo, endereco)


    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    elif tipo == "GET":

        nome_arquivo = partes[1]

        # GET|nome|1,5,9 = pede só os pedaços que faltam
        so_estes = None

        if len(partes) > 2 and partes[2]:

            so_estes = [int(n) for n in partes[2].split(",") if n.isdigit()]

        # Em outra thread: enviar um arquivo grande não pode travar o recebimento
        threading.Thread(
            target=enviar_arquivo,
            args=(nome_arquivo, endereco, so_estes),
            daemon=True
        ).start()


    # --------------------------------------------------------
    # DATA
    # --------------------------------------------------------

    elif tipo == "DATA":

        receber_dados(
            partes,
            endereco
        )


    # --------------------------------------------------------
    # REMOVE
    # --------------------------------------------------------

    elif tipo == "REMOVE":

        nome_arquivo = partes[1]

        caminho = os.path.join(
            PASTA,
            nome_arquivo
        )

        # Cancela pedido ou transferência em andamento desse arquivo
        pedidos_pendentes.pop(nome_arquivo, None)
        arquivos_recebidos.pop(nome_arquivo, None)

        if os.path.exists(caminho):

            arquivos_removidos_rede.add(nome_arquivo)

            os.remove(caminho)

            print(
                f"[REMOVIDO] {nome_arquivo}"
            )

    # --------------------------------------------------------
    # LIST
    # --------------------------------------------------------

    elif tipo == "LIST":

        arquivos = listar_arquivos()

        mensagem = "FILES|" + ",".join(arquivos)

        sock.sendto(
            mensagem.encode(),
            endereco
        )


    # --------------------------------------------------------
    # FILES
    # --------------------------------------------------------

    elif tipo == "FILES":

        arquivos = partes[1].split(",")

        for arquivo in arquivos:

            if arquivo:

                pedir_arquivo(arquivo, endereco)


    # --------------------------------------------------------
    # HEARTBEAT
    # --------------------------------------------------------

    elif tipo == "HEARTBEAT":

        remetente = partes[1]

        lista = partes[2] if len(partes) > 2 else ""

        # Guarda que o peer está vivo e quais arquivos ele tem
        estado_peers[remetente] = {
            "ultimo": time.time(),
            "arquivos": set(a for a in lista.split(",") if a)
        }


# ============================================================
# ENVIO DO ARQUIVO
# ============================================================

def enviar_arquivo(nome_arquivo, endereco, so_estes=None):

    caminho = os.path.join(
        PASTA,
        nome_arquivo
    )

    # Verifica se o arquivo existe
    if not os.path.exists(caminho):
        return

    print(
        f"[ENVIANDO] {nome_arquivo}"
    )

    # Abre o arquivo no modo binário
    with open(caminho, "rb") as arquivo:

        # Lê todo o conteúdo do arquivo
        dados_arquivo = arquivo.read()

    # Calcula quantos pedaços serão necessários
    total_pedacos = (
        len(dados_arquivo) + CHUNK_SIZE - 1
    ) // CHUNK_SIZE

    # Se vieram números de pedaços (pedido de reenvio), manda só esses
    numeros = so_estes if so_estes is not None else range(total_pedacos)

    # Percorre o arquivo pedaço por pedaço
    for numero_pedaco in numeros:

        if numero_pedaco >= total_pedacos:
            continue

        inicio = numero_pedaco * CHUNK_SIZE

        fim = inicio + CHUNK_SIZE

        pedaco = dados_arquivo[inicio:fim]

        # Monta o cabeçalho da mensagem
        cabecalho = (
            f"DATA|{nome_arquivo}|"
            f"{numero_pedaco}|{total_pedacos}|"
        ).encode()

        # Junta o cabeçalho com os bytes do arquivo
        mensagem = cabecalho + pedaco

        # Envia o pedaço pelo UDP
        sock.sendto(
            mensagem,
            endereco
        )

        print(
            f"[ENVIO] pedaço "
            f"{numero_pedaco + 1}/{total_pedacos}"
        )

        time.sleep(0.001) # Pequena pausa para não encher o buffer de quem recebe


# ============================================================
# RECEBIMENTO DO ARQUIVO
# ============================================================

# Guarda temporariamente os pedaços dos arquivos recebidos
arquivos_recebidos = {}


def receber_dados(dados, endereco):

    # Divide a mensagem somente até o quarto "|"
    partes = dados.split(b"|", 4)

    # Verifica se a mensagem possui todas as partes
    if len(partes) != 5:
        return

    # Converte as partes do cabeçalho para texto
    tipo = partes[0].decode()
    nome_arquivo = partes[1].decode()
    numero_pedaco = int(partes[2].decode())
    total_pedacos = int(partes[3].decode())

    # O conteúdo do arquivo continua sendo bytes
    pedaco = partes[4]

    # Pedaço repetido de um arquivo que já está completo: ignora
    if (
        os.path.exists(os.path.join(PASTA, nome_arquivo))
        and nome_arquivo not in arquivos_recebidos
    ):
        return

    print(
        f"[RECEBIDO] {nome_arquivo} "
        f"pedaço {numero_pedaco + 1}/{total_pedacos}"
    )

    # Cria a estrutura para esse arquivo
    if nome_arquivo not in arquivos_recebidos:

        arquivos_recebidos[nome_arquivo] = {
            "total": total_pedacos,
            "pedacos": {},
            "inicio": time.time(), # Quando começou a chegar
            "ultimo": time.time(), # Quando chegou o último pedaço
            "origem": endereco # Quem está enviando (para pedir os pedaços que faltarem)
        }

    # Salva o pedaço recebido
    arquivos_recebidos[nome_arquivo]["pedacos"][
        numero_pedaco
    ] = pedaco

    # O arquivo começou a chegar: o pedido deixa de estar pendente
    arquivos_recebidos[nome_arquivo]["ultimo"] = time.time()
    pedidos_pendentes.pop(nome_arquivo, None)

    # Verifica quantos pedaços já foram recebidos
    quantidade_recebida = len(
        arquivos_recebidos[nome_arquivo]["pedacos"]
    )

    # Se recebeu todos os pedaços
    if quantidade_recebida == total_pedacos:

        # Marca que o arquivo veio da rede
        arquivos_recebidos_rede.add(nome_arquivo)

        caminho = os.path.join(
            PASTA,
            nome_arquivo
        )

        # Cria o arquivo final
        with open(caminho, "wb") as arquivo:

            for numero in range(total_pedacos):

                arquivo.write(
                    arquivos_recebidos[nome_arquivo]["pedacos"][numero]
                )

        print(
            f"[ARQUIVO COMPLETO] {nome_arquivo}"
        )

        # Remove os pedaços da memória
        del arquivos_recebidos[nome_arquivo]


# ============================================================
# SINCRONIZAÇÃO INICIAL
# ============================================================

def sincronizar():

    print("[SYNC] Solicitando arquivos aos peers...")

    mensagem = "LIST"

    # Enviado 3 vezes: se o pedido ou a resposta se perderem, o peer novo ainda se atualiza
    # (pedir_arquivo evita pedir o mesmo arquivo mais de uma vez)
    for _ in range(3):

        enviar_para_todos(mensagem)

        time.sleep(1)


# ============================================================
# HEARTBEAT
# ============================================================

def heartbeat():

    while True:

        # Avisa aos outros peers que estou vivo e quais arquivos eu tenho
        lista = ",".join(sorted(listar_arquivos()))

        enviar_para_todos(f"HEARTBEAT|{PEER_ID}|{lista}")

        time.sleep(INTERVALO_HEARTBEAT)


# ============================================================
# REPETIÇÃO DE PEDIDOS (tolerância a perda de pacotes)
# ============================================================

def repetidor():

    while True:

        time.sleep(1)

        agora = time.time()

        # 1) Arquivos pedidos que ainda não começaram a chegar
        #    (o GET ou todos os primeiros pedaços se perderam)
        for nome, pedido in list(pedidos_pendentes.items()):

            if os.path.exists(os.path.join(PASTA, nome)):
                pedidos_pendentes.pop(nome, None)
                continue

            if agora - pedido["ultimo"] <= PAUSA_REPETIR:
                continue

            if pedido["tentativas"] >= MAX_TENTATIVAS:
                pedidos_pendentes.pop(nome, None)
                print(f"[DESISTIU] {nome}: sem resposta")
                continue

            pedido["tentativas"] += 1
            pedido["ultimo"] = agora

            print(f"[PEDINDO DE NOVO] {nome}")

            try:
                sock.sendto(f"GET|{nome}".encode(), pedido["endereco"])
            except OSError:
                pass

        # 2) Arquivos que começaram a chegar mas ficaram incompletos:
        #    pede só os pedaços que faltam
        for nome, info in list(arquivos_recebidos.items()):

            if agora - info["inicio"] > TIMEOUT_TRANSFERENCIA:
                continue

            if agora - info["ultimo"] <= PAUSA_REPETIR:
                continue

            faltando = [
                n for n in range(info["total"])
                if n not in info["pedacos"]
            ][:200]

            if not faltando:
                continue

            info["ultimo"] = agora

            print(f"[PEDINDO PEDAÇOS] {nome}: faltam {len(faltando)}")

            try:
                sock.sendto(
                    f"GET|{nome}|{','.join(str(n) for n in faltando)}".encode(),
                    info["origem"]
                )
            except OSError:
                pass


# ============================================================
# PAINEL (informações sumarizadas)
# ============================================================

def peer_ativo(peer_id):

    return time.time() - estado_peers[peer_id]["ultimo"] < TIMEOUT_PEER


def montar_painel():

    meus = listar_arquivos()

    ids = [PEER_ID] + [p["id"] for p in PEERS]

    arquivos_de = {PEER_ID: meus}
    ativos = {PEER_ID: True}

    for p in PEERS:
        ativos[p["id"]] = peer_ativo(p["id"])
        arquivos_de[p["id"]] = set(estado_peers[p["id"]]["arquivos"])

    online = [i for i in ids if ativos[i]]

    sincronizado = all(arquivos_de[i] == meus for i in online)

    linhas = []
    linhas.append("=" * 60)
    linhas.append(f" PEER {PEER_ID} | {HOST}:{PORT} | {time.strftime('%H:%M:%S')}")
    linhas.append("=" * 60)
    linhas.append("")
    linhas.append(f" PEERS ATIVOS: {len(online)}/{len(ids)}")
    linhas.append(f"   {'PEER':<9}{'ENDERECO':<24}{'ESTADO':<10}ARQUIVOS")
    linhas.append(f"   {PEER_ID + ' (eu)':<9}{HOST + ':' + str(PORT):<24}{'ATIVO':<10}{len(meus)}")

    for p in PEERS:
        i = p["id"]
        estado = "ATIVO" if ativos[i] else "INATIVO"
        qtd = str(len(arquivos_de[i])) if ativos[i] else "-"
        linhas.append(f"   {i:<9}{p['host'] + ':' + str(p['port']):<24}{estado:<10}{qtd}")

    linhas.append("")
    linhas.append(" ARQUIVOS POR PEER  (OK = tem, -- = nao tem, ?? = peer inativo)")

    todos = set()
    for i in online:
        todos |= arquivos_de[i]

    linhas.append(f"   {'ARQUIVO':<28}" + "".join(f"{i:^6}" for i in ids))

    if not todos:
        linhas.append("   (nenhum arquivo na rede)")

    for nome in sorted(todos):
        linha = f"   {nome[:27]:<28}"
        for i in ids:
            if not ativos[i]:
                marca = "??"
            else:
                marca = "OK" if nome in arquivos_de[i] else "--"
            linha += f"{marca:^6}"
        linhas.append(linha)

    linhas.append("")
    linhas.append(f" REDE SINCRONIZADA: {'SIM' if sincronizado else 'NAO (sincronizando...)'}")
    linhas.append("")
    linhas.append(" ULTIMOS EVENTOS:")

    for e in list(eventos)[-8:]:
        linhas.append(f"   {e[:70]}")

    return "\n".join(linhas)


def painel():

    os.system("") # Habilita códigos ANSI no terminal do Windows

    while True:

        # \033[2J limpa a tela e \033[H volta o cursor para o topo
        saida_original.write("\033[2J\033[H" + montar_painel() + "\n")
        saida_original.flush()

        time.sleep(1)


# ============================================================
# INICIALIZAÇÃO
# ============================================================

thread_servidor = threading.Thread(
    target=servidor,
    daemon=True
)

thread_servidor.start()


thread_monitor = threading.Thread(
    target=monitorar_pasta,
    daemon=True
)

thread_monitor.start()


thread_heartbeat = threading.Thread(
    target=heartbeat,
    daemon=True
)

thread_heartbeat.start()


thread_repetidor = threading.Thread(
    target=repetidor,
    daemon=True
)

thread_repetidor.start()


# Sincronização quando o peer inicia
sincronizar()


print("Peer executando...")


if MODO_PAINEL:

    painel()

while True:

    time.sleep(1)