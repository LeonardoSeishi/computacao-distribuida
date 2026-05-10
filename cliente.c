#include "cliente.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define INTERCEPTADOR_HOST "127.0.0.1"
#define INTERCEPTADOR_PORTA 9000
#define BUFFER_SIZE 4096

int comunicar_com_interceptador(char *mensagem, char *resposta) {
    int sockfd;
    struct sockaddr_in address;

    // cria um socket TCP e envia a msg ao interceptador
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        perror("Erro ao criar socket");
        return -1;
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(INTERCEPTADOR_HOST);
    address.sin_port = htons(INTERCEPTADOR_PORTA);

    if (connect(sockfd, (struct sockaddr *)&address, sizeof(address)) == -1) {
        perror("Erro ao conectar ao interceptador");
        close(sockfd);
        return -1;
    }
    write(sockfd, mensagem, strlen(mensagem));
    int n = read(sockfd, resposta, BUFFER_SIZE - 1);
    if (n <= 0) {
        close(sockfd);
        return -1;
    }
    resposta[n] = '\0';
    close(sockfd);
    return 0;
}

int encurta(char *url_original, char *url_curta) {
    char mensagem[BUFFER_SIZE];
    char resposta[BUFFER_SIZE];
    // monta comando ENCURTA e envia ao interceptador
    snprintf(mensagem, sizeof(mensagem), "ENCURTA %s\n", url_original);
    if (comunicar_com_interceptador(mensagem, resposta) != 0) {
        return -1;
    }
    if (strncmp(resposta, "OK ", 3) == 0) {
        strcpy(url_curta, resposta + 3);
        url_curta[strcspn(url_curta, "\n")] = '\0';
        return 0;
    }
    return -1;
}

int resolve(char *codigo_curto, char *url_original) {
    char mensagem[BUFFER_SIZE];
    char resposta[BUFFER_SIZE];
    // monta comando RESOLVE e envia ao interceptador
    snprintf(mensagem, sizeof(mensagem), "RESOLVE %s\n", codigo_curto);
    if (comunicar_com_interceptador(mensagem, resposta) != 0) {
        return -1;
    }
    if (strncmp(resposta, "OK ", 3) == 0) {
        strcpy(url_original, resposta + 3);
        url_original[strcspn(url_original, "\n")] = '\0';
        return 0;
    }
    return -1;
}

int remove_url(char *codigo_curto) {
    char mensagem[BUFFER_SIZE];
    char resposta[BUFFER_SIZE];
    // monta comando REMOVE e envia ao interceptador
    snprintf(mensagem, sizeof(mensagem), "REMOVE %s\n", codigo_curto);
    if (comunicar_com_interceptador(mensagem, resposta) != 0) {
        return -1;
    }
    if (strncmp(resposta, "OK", 2) == 0) {
        return 0;
    }
    return -1;
}
