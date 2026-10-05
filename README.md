# Sincronização de arquivos P2P via UDP

**Disciplina:** Redes de Computadores: Aplicação e Transporte

Grupo: Artur Marques, Davi Gomes, Murilo Oliveira

Sistema de atualização de arquivos distribuídos entre vários nodos (*peers*), usando o protocolo UDP, em Python. Cada peer é ao mesmo tempo servidor e cliente: ele vigia a sua pasta `tmp` e mantém o conteúdo dela igual ao dos demais. Um arquivo colocado em um peer aparece nos outros, e um arquivo apagado em um peer some de todos.

## Como funciona
- A rede é estática: cada peer conhece os outros por uma lista fixa de IP e porta, no arquivo `config.json`. Não há autenticação nem descoberta automática.
- O peer verifica a pasta `tmp` a cada 2 segundos. Se um arquivo entrou, ele anuncia aos outros. Se um arquivo saiu, ele avisa a remoção.
- Quem recebe um anúncio e ainda não tem o arquivo pede o conteúdo, que chega dividido em pedaços de 1024 bytes.
- Um peer novo, com `tmp` vazio, pede a lista de arquivos aos demais e se atualiza sozinho.
- Cada peer manda a cada 2 segundos um heartbeat. Com isso cada peer sabe quem está ativo e o que cada um tem, e mostra isso em um painel.

## Estrutura do projeto

```
TGARC/
├── docker-compose.yml        sobe os peers em containers
├── peerA/
│   ├── peer.py               programa do peer (igual em todas as pastas)
│   ├── config.json           configuração para rodar sem Docker (127.0.0.1)
│   ├── config.docker.json    configuração para rodar no Docker
│   └── tmp/                  pasta sincronizada (criada automaticamente)
├── peerB/  ...
├── peerC/  ...
└── peerD/  ...               peer que entra depois, para demonstrar um novo nodo
```

O `peer.py` é o mesmo em todas as pastas. O que muda de um peer para outro é o arquivo de configuração.

### Configuração

```json
{
    "peer_id": "A",
    "host": "127.0.0.1",
    "port": 5000,
    "peers": [
        { "id": "B", "host": "127.0.0.1", "port": 5001 }
    ]
}
```

| Campo | Significado |
|---|---|
| `peer_id` | identificador deste peer |
| `host` / `port` | endereço deste peer, usado pelos outros para falar com ele |
| `peers` | lista dos demais peers (id, host, porta) |
| `bind` | opcional. IP onde o socket escuta. Se omitido, usa `host`. Em container ou VM usa-se `0.0.0.0` |

O arquivo lido pode ser trocado pela variável de ambiente `CONFIG` (é o que o Docker faz, usando o `config.docker.json`). Para acrescentar um peer, basta incluí-lo na lista dos outros.

## Protocolo

Mensagens de texto sobre UDP, separadas por `|`.

| Mensagem | Função |
|---|---|
| `ANNOUNCE\|nome` | o peer avisa que passou a ter um arquivo |
| `GET\|nome` | pede o arquivo inteiro |
| `GET\|nome\|1,5,9` | pede apenas os pedaços que faltam |
| `DATA\|nome\|n\|total\|bytes` | envia o pedaço `n` de `total` |
| `REMOVE\|nome` | o arquivo foi apagado |
| `LIST` | pede a lista de arquivos (usado por um peer que acabou de entrar) |
| `FILES\|a,b,c` | resposta ao `LIST` |
| `HEARTBEAT\|id\|a,b,c` | "estou vivo" e os arquivos que tenho |

## Tolerância às falhas do UDP

O UDP pode perder, duplicar e embaralhar datagramas. A própria aplicação trata isso:

| Problema | Como é tratado |
|---|---|
| Perda de `ANNOUNCE` ou `REMOVE` | cada aviso é enviado 5 vezes quem recebe só pede o arquivo uma vez |
| Perda do `GET` ou dos primeiros pedaços | o pedido é repetido após 2,5 s sem resposta, até 10 vezes |
| Arquivo que ficou incompleto | o receptor calcula quais pedaços faltam e pede só eles (`GET\|nome\|1,5,9`) |
| Pedaço duplicado | os pedaços ficam em um dicionário por número, então o repetido só sobrescreve; pedaço de arquivo já completo é ignorado |
| Pedaços fora de ordem | a remontagem é pelo número do pedaço |
| Perda do `LIST` ou da resposta | o `LIST` é enviado 3 vezes |
| Transferência que nunca termina | depois de 60 s ela é considerada abandonada |

## Painel

Com o argumento `painel`, o peer mostra uma tela atualizada a cada segundo:

```
============================================================
 PEER A | peera:5000 | 15:25:58
============================================================

 PEERS ATIVOS: 3/4
   PEER     ENDERECO                ESTADO    ARQUIVOS
   A (eu)   peera:5000              ATIVO     2
   B        peerb:5000              ATIVO     2
   C        peerc:5000              ATIVO     2
   D        peerd:5000              INATIVO   -

 ARQUIVOS POR PEER  (OK = tem, -- = nao tem, ?? = peer inativo)
   ARQUIVO                       A     B     C     D
   relatorio.pdf                 OK    OK    OK    ??
   foto.bin                      OK    OK    OK    ??

 REDE SINCRONIZADA: SIM

 ULTIMOS EVENTOS:
   ...
```

Um peer sem heartbeat há mais de 6 segundos aparece como `INATIVO`. A linha "REDE SINCRONIZADA" é `SIM` quando todos os peers ativos têm o mesmo conjunto de arquivos. Sem o argumento `painel`, o programa mostra o log bruto.

## Como executar

### Sem Docker

Requer Python 3. Abra um terminal por peer, cada um dentro da pasta do seu peer:

```
cd peerA
python peer.py painel
```

```
cd peerB
python peer.py painel
```

```
cd peerC
python peer.py painel
```

Cada peer usa o seu `config.json`, com portas 5000 (A), 5001 (B), 5002 (C) e 5003 (D) em `127.0.0.1`.

### Docker (cada peer em um container)

Requer o Docker Desktop aberto. Em um terminal, dentro da pasta do projeto (a que tem o `docker-compose.yml`):

```
docker compose up -d
```

Isso sobe os peers A, B e C, cada um com IP próprio na rede do Docker. Para ver o painel de cada um, em terminais separados:

```
docker attach peer_a
docker attach peer_b
docker attach peer_c
```

Para sair do painel sem derrubar o container, use `Ctrl+P` e depois `Ctrl+Q`. Não use `Ctrl+C`, que encerra o peer.

Os arquivos ficam nas pastas `peerA/tmp`, `peerB/tmp` e `peerC/tmp` do computador, montadas dentro dos containers. É só copiar arquivos para elas.

**Entrada de um novo peer:** o peer D já consta na lista dos outros e aparece como `INATIVO` até ser iniciado.

```
docker compose --profile novo up -d peerd
docker attach peer_d
```

Ele começa com o `tmp` vazio e recebe sozinho os arquivos que já existem na rede.

**Parar:**

```
docker compose --profile novo down
```

As pastas `tmp` não são apagadas.

### Em máquinas diferentes

1. Copie a pasta de um peer para cada máquina.
2. No `config.json` de cada uma, troque `127.0.0.1` pelo IP real de cada máquina, em `host` e na lista `peers`.
3. Libere a porta UDP no firewall (PowerShell como administrador, ajustando a porta):
   ```
   New-NetFirewallRule -DisplayName "P2P UDP" -Direction Inbound -Protocol UDP -LocalPort 5000 -Action Allow
   ```
4. Rode `python peer.py painel` em cada máquina. Se o peer não receber nada, acrescente `"bind": "0.0.0.0"` ao `config.json`.

## Como testar

1. **Adição:** copie um arquivo para o `tmp` do peer A. Em poucos segundos ele aparece no `tmp` de B e C, e o painel mostra `OK` nas três colunas.
2. **Remoção:** apague o arquivo no `tmp` de B. Ele some dos três.
3. **Novo peer:** inicie o D com o `tmp` vazio. Ele passa a `ATIVO` nos painéis e recebe todos os arquivos existentes.
4. **Peer inativo:** pare um container (`docker stop peer_c`). Em cerca de 6 segundos os outros o mostram como `INATIVO`. Para voltar: `docker start peer_c`.

## Critérios de avaliação

| Critério | Onde está |
|---|---|
| Servidor | `servidor()` e `processar_mensagem()`: recebem e tratam as mensagens e enviam os arquivos |
| Cliente | `pedir_arquivo()`, `receber_dados()` e `repetidor()`: pedem, recebem e remontam os arquivos |
| Integração cliente e servidor | todo peer executa as duas funções em threads, no mesmo programa |
| Adição de arquivos | `monitorar_pasta()` detecta o arquivo novo e envia `ANNOUNCE` |
| Remoção de arquivos | `monitorar_pasta()` detecta o arquivo removido e envia `REMOVE` |
| Lista dos arquivos | `LIST` e `FILES`; coluna de arquivos por peer no painel |
| Informações sumarizadas | `montar_painel()` e `painel()`, alimentados pelo `HEARTBEAT` |
| Ambientes diferentes | `docker-compose.yml`: um container por peer, com IP próprio |
| Novo nodo na rede | peer D: `sincronizar()` envia `LIST` e baixa os arquivos que faltam |
