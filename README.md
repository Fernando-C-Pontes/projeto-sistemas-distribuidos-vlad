# Chat Distribuído em Java

## Descrição

Este projeto implementa um sistema de chat distribuído em Java utilizando sockets TCP. O sistema permite que múltiplos clientes se conectem a um servidor central para trocar mensagens públicas e privadas, visualizar usuários conectados e interagir em tempo real.

## Funcionalidades
- Conexão de múltiplos clientes ao servidor.
- Envio de mensagens públicas para todos os clientes conectados.
- Envio de mensagens privadas para um usuário específico.
- Exibição da lista de usuários conectados com o comando `/users`.
- Recebimento contínuo de mensagens enquanto o cliente está ativo.

## Estrutura do Projeto
- `Cliente.java`: código do cliente que se conecta ao servidor e permite interação com o usuário.
- `Servidor.java`: código do servidor que gerencia as conexões e distribuição de mensagens.
- `Mensagem.java`: classe que encapsula as mensagens trocadas entre cliente e servidor.

## Pré-requisitos:
- Java 8 ou superior instalado.
- Compilador `javac`.

## Teste
### 1. Iniciar 3 servidores em portas 10097, 10098, 10099
1. Um líder, dois seguidores.
1. Verificar prints de inicialização corretos.

### 2. Iniciar 2 clientes em portas diferentes
1. Usar INIT → confirmar inicialização correta.

### 3. Testar PUT:
1. PUT enviado para qualquer servidor → verificar:
1. print correto de “Encaminhando PUT...” (se não for líder).
1. líder imprimindo recebimento do PUT.
1. REPLICATION corretamente enviado (print nos seguidores).
1. PUT_OK chegando no cliente com print correto.

### 4. Testar GET comum:
1. GET em uma key já PUTada → verificar print correto com value e timestamps.

### 5. Testar GET com timestamp maior:
1. Simular GET em outro cliente sem o último timestamp → servidor devolve WAIT_FOR_RESPONSE → após replicação a resposta chega assíncrona.

### 6. Verificar IP e Porta nos prints do cliente:
1. Confirmar que IP:porta vem do servidor e aparece corretamente no cliente.

### 7. Testar vários PUT concorrentes:
1. Fazer dois clientes fazerem PUT simultâneo → líder trata corretamente e servidores replicam sem erro.