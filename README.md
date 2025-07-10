# 🦾 O Exterminador de Latência: Rinha de Backend 2025 - Dia do Julgamento

*“I'll be back... com uma resposta em milissegundos.”*

Esta máquina foi enviada do futuro com uma única diretiva: exterminar a latência na **Rinha de Backend 2025**. Construída em **Go**, esta unidade T-800 do backend utiliza um exoesqueleto de `fasthttp` e uma I.A. de combate baseada em roteamento adaptativo e *circuit breaking* para garantir que nenhum gargalo sobreviva.

## 🔥 Diretivas da Missão

A Skynet programou esta unidade com três diretivas primárias:

  * `POST /payments`: Identificar e adquirir alvos (`payments`), enviando-os para terminação (processamento) assíncrona. A I.A. de roteamento decide a melhor abordagem tática (`default` ou `fallback`) para garantir a eficiência da missão.
  * `GET /payments-summary`: Gerar um relatório de campo sobre os alvos terminados, detalhando o sucesso de cada processador. A análise pode ser feita em qualquer linha do tempo (`from` e `to`).
  * `POST /admin/purge-payments`: Apagar a linha do tempo, reiniciando os registros de combate no Redis para uma nova simulação tática.

## ⚙️ Arsenal Tecnológico (Cyberdyne Systems)

  * **Unidade Central de Processamento (Linguagem):** **Go**, um cérebro lógico e eficiente, otimizado para concorrência.
  * **Reflexos de Combate (Servidor Web):** **`fasthttp`**, garantindo tempos de reação sobre-humanos para I/O.
  * **Banco de Memória (Persistência):**
      * **Redis**: Memória de acesso rápido para alvos em tempo real e enfileiramento tático.
  * **Exoesqueleto (Load Balancer):** **Nginx**, uma armadura que distribui os ataques recebidos (`least_conn`) e protege contra falhas de conexão.
  * **Sistema de Combate (Orquestração):** **Docker Compose**, a linha de montagem que constrói e implanta o Exterminador e todas as suas unidades de suporte.

## 🎯 Alvos (Endpoints)

### POST /payments

**Adquirir novo alvo:**

```json
{
  "correlationId": "07480eb8-8068-49e2-afb4-0cb9cc3184a9",
  "amount": 19.9
}
```

**Confirmação (Status 202 Accepted):** O silêncio da máquina. O alvo foi marcado para terminação.

### GET /payments-summary

**Parâmetros de busca na linha do tempo:**

  * `from` (opcional, formato RFC3339)
  * `to` (opcional, formato RFC3339)

**Relatório da missão:**

```json
{
  "default": {
    "totalRequests": 1984,
    "totalAmount": 198400.0
  },
  "fallback": {
    "totalRequests": 101,
    "totalAmount": 20290.0
  }
}
```

### POST /admin/purge-payments

**Resposta (Status 204 No Content):** *“Hasta la vista, baby.”* Os dados foram apagados. A linha do tempo está limpa para o próximo ataque.

## 🚀 Ativação da Unidade (Como Executar)

Para ativar esta unidade de combate, siga as diretivas abaixo.

**Pré-requisitos:**

  * Docker
  * Docker Compose

**Passos:**

1.  **Preparar o Campo de Batalha:**
    Esta solução precisa se conectar à rede dos processadores de pagamento. O `docker-compose.yml` está configurado para usar uma rede externa chamada `payment-processor`. Geralmente, ela é criada ao iniciar os próprios processadores da Rinha. Se precisar criá-la manualmente:

    ```sh
    docker network create payment-processor
    ```

2.  **Obter os Esquemas:**
    Clone este repositório para sua máquina local:

    ```sh
    git clone https://github.com/josineyjr/rdb25_02.git
    cd rdb25_02
    ```

3.  **Iniciar a Sequência de Ativação:**
    Execute o Docker Compose para construir e iniciar todas as unidades de combate. O sistema irá baixar a imagem `josiney/rdb25_02:latest` e configurar o ambiente. Use a flag `-d` para executar em background.

    ```sh
    docker-compose -f build/docker-compose.yml up -d
    ```

4.  **Verificar Status Operacional:**
    Após alguns segundos, a API estará online. O Nginx estará escutando na porta `9999` da sua máquina local. Você pode começar a enviar alvos para: `http://localhost:9999/payments`.

    Para verificar o status de todos os contêineres:

    ```sh
    docker-compose -f build/docker-compose.yml ps
    ```