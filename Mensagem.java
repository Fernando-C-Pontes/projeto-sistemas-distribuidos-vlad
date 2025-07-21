import java.io.Serializable;
 
public class Mensagem implements Serializable {
    private String type; // Tipo da mensagem: "PUT", "GET", "PUT_OK", "WAIT_FOR_RESPONSE"
    private String key; // Chave da mensagem
    private String value; // Valor da mensagem (pode ser null para GET)
    private int timestamp; // Timestamp da mensagem (para controle de versão)
    private String ip;  // IP do servidor que responde (para PUT_OK e GET)
    private int porta;  // Porta do servidor que responde (para PUT_OK e GET)

    // Construtor vazio
    public Mensagem() {}

    // Construtor completo: inicializa todos os campos
    public Mensagem(String type, String key, String value, int timestamp, int porta) {
        this.type = type; 
        this.key = key; 
        this.value = value; 
        this.timestamp = timestamp;
        this.porta = porta;
    }

    // Getters 
    public String getType() { return type; } // Retorna o tipo da mensagem
    public String getKey() { return key; } // Retorna a chave da mensagem
    public String getValue() { return value; } // Retorna o valor da mensagem
    public int getTimestamp() { return timestamp; } // Retorna o timestamp da mensagem
    public String getIp() { return ip; } // Retorna o IP do servidor que responde
    public int getPorta() { return porta; } // Retorna a porta do servidor que responde

    // Setters para ip e porta do servidor (resposta GET/PUT_OK vêm com ip e porta do servidor)
    public void setIp(String ip) { this.ip = ip; } // Define o IP do servidor que responde
    public void setPorta(int porta) { this.porta = porta; } // Define a porta do servidor que responde

    @Override
    public String toString() {  
        return "Mensagem{" +
                "type='" + type + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", ip='" + ip + '\'' +
                ", porta=" + porta +
                '}';
    }
}
