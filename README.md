### 🚀 O Papa-Léguas da Rinha — Go + gnet

Implementação do desafio feita em Go, voando baixo com `gnet` e um roteamento adaptativo para não deixar nenhuma transação para trás\!

**🛠️ Tecnologias Utilizadas**

  * **Linguagem:** Go
  * **Servidor HTTP:** `gnet`
  * **Comunicação entre serviços:** UDP
  * **Armazenamento:** Em memória com Ring Buffer
  * **Load Balancer:** HAProxy
  * **Orquestração:** Docker + Docker Compose

**Visão Geral da Arquitetura**

Esta versão do projeto evolui para uma arquitetura com um **worker stateful**, eliminando a necessidade de um serviço de Redis externo e otimizando a comunicação entre os componentes.

  * **API Gateway (`gnet`)**: O ponto de entrada para as requisições de pagamento continua sendo um servidor HTTP de alta performance com `gnet`. No entanto, em vez de publicar em um tópico Redis, a API agora envia os dados do pagamento diretamente para o worker via **UDP**, uma escolha que privilegia a velocidade e a baixa sobrecarga.

  * **Worker Stateful**: O worker agora é o coração do sistema. Ele não apenas processa as transações, mas também **armazena o estado da aplicação em memória**. Ele recebe os dados de pagamento da API via UDP e os processa de forma assíncrona.

  * **Armazenamento em Memória (Ring Buffer)**: Para agregar os dados de pagamento para os resumos, o worker utiliza um **Ring Buffer** em memória. Essa abordagem de armazenamento elimina a latência de rede que existia com o Redis, tornando o acesso aos dados extremamente rápido.

  * **Roteamento Adaptativo e Circuit Breaker**: A lógica de roteamento adaptativo e *Circuit Breaker* é mantida no worker. Ele monitora a saúde dos processadores de pagamento (default e fallback) e direciona o tráfego de acordo, garantindo a resiliência do sistema.

  * **Servidor HTTP no Worker**: O worker agora também possui seu próprio servidor `gnet` embutido, que expõe os endpoints `/payments-summary` e `/purge-payments`.

  * **Load Balancer (HAProxy)**: O HAProxy foi reconfigurado para lidar com a nova arquitetura. Ele direciona as requisições de criação de pagamento (`/payments`) para as APIs e as requisições de consulta (`/payments-summary`, `/purge-payments`) diretamente para o servidor HTTP do worker.

**Como Executar o Projeto**

1.  **Pré-requisitos**:

      * Docker
      * Docker Compose

2.  **Clone o repositório:**

    ```bash
    git clone https://github.com/JosineyJr/rdb25_02.git
    cd rdb25_02
    git checkout feature/stateful-worker
    ```

3.  **Inicie os serviços com Docker Compose:**

    ```bash
    docker-compose up -d --build
    ```

4.  **A API estará disponível em:** `http://localhost:9999`

**Endpoints da API**

  * `POST /payments`: Envia uma nova transação de pagamento.
  * `GET /payments-summary`: Retorna um resumo dos pagamentos processados.
  * `POST /purge-payments`: Limpa os dados de resumo dos pagamentos.