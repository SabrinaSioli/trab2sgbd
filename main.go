package main

import (
	"fmt"
	"strconv"
	"strings"
)

/*

	STRUCTS PARA ESTRUTURAS DE DADOS

*/

// Representa uma transação (item de TrManager)
type TrManagerItem struct {
	label  int // nome da transação
	trID   int
	status int // Status: 0-> ativa; 1-> concluída; 2-> abortada; 3-> esperando.
	espera []*LockTableItem
}

// Representa um bloqueio (item de LockTable)
type LockTableItem struct {
	idItem  string
	trLabel int //nome da transação
	trID    int
	duracao int // Duração: 0-> curta; 1-> longa.
	tipo    int // Tipo: 0-> leitura; 1-> escrita.
}

// Representa uma tupla <item, lista de bloqueios aguardando> (compõe estrutura WaitFor)
type WaitForItem struct {
	idItem    string
	operacoes []*LockTableItem
}

// Aresta direcionada do grafo de espera
type Aresta struct {
	origem  int
	destino int
}

/*

	OPERAÇÕES DE ESCALONAMENTO (BEGIN, READ, WRITE E COMMIT)

*/

// Iniciar transação (BT)
func op_BT(trManager *[]*TrManagerItem, label int) {

	trID := len(*trManager) //timestamp

	transacao := TrManagerItem{
		label:  label, //nome da transação
		trID:   trID,
		status: 0,
	}

	*trManager = append(*trManager, &transacao)
	str := strconv.Itoa(label)
	saida = saida + "BT(" + str + ") "
}

// Tenta escalonar operação de leitura (READ)
func op_rl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID && transacao.status == 0 {
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID && bloqueio.tipo == 1 {
					return bloqueio.trID
				} else if bloqueio.idItem == operacao.idItem && bloqueio.trID == operacao.trID && bloqueio.tipo == 1 {
					fmt.Printf("A transação %d já possui bloqueio exclusivo\n", transacao.label)
					str := strconv.Itoa(operacao.trLabel)
					saida = saida + "R" + str + "(" + operacao.idItem + ") "
					return -2
				}

			}
			fmt.Printf("Bloqueio de LEITURA OBTIDO: T%d no item [%s] \n", operacao.trLabel, operacao.idItem)
			str := strconv.Itoa(operacao.trLabel)
			saida = saida + "R" + str + "(" + operacao.idItem + ") "
			*lockTable = append(*lockTable, operacao)

			if operacao.duracao == 0 {
				op_ul(trManager, lockTable, waitFor, grafoEspera, operacao.trID, operacao.idItem)
			}

			return -1

		}
	}

	return -1

}

// Tenta escalonar operação de escrita (WRITE)
func op_wl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		//se transação estiver ATIVA
		if transacao.trID == operacao.trID && transacao.status == 0 {
			//se houver outro bloqueio no mesmo item => vai pra WAIT DIE retornando o timestamp do bloqueio que o barrou
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID {
					return bloqueio.trID
				} else if bloqueio.idItem == operacao.idItem && bloqueio.trID == operacao.trID && bloqueio.tipo == 0 {
					bloqueio.tipo = 1
					fmt.Printf("A transação %d CONVERTEU bloqueio de LEITURA em ESCRITA no item [%s]", transacao.label, bloqueio.idItem)
					str := strconv.Itoa(operacao.trLabel)
					saida = saida + "W" + str + "(" + operacao.idItem + ") "
					return -2
				}

			}
			//se não houve conflito, o bloqueio é obtido
			fmt.Printf("Bloqueio de ESCRITA OBTIDO: T%d no item [%s] \n", operacao.trLabel, operacao.idItem)
			str := strconv.Itoa(operacao.trLabel)
			saida = saida + "W" + str + "(" + operacao.idItem + ") "
			*lockTable = append(*lockTable, operacao)

			if operacao.duracao == 0 {
				op_ul(trManager, lockTable, waitFor, grafoEspera, operacao.trID, operacao.idItem)
			}

			return -1

		}
	}
	return -1
}

// Tenta escalonar operação de COMMIT
func op_C(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, trID int, trLabel int) {

	for _, transacao := range *trManager {
		if transacao.trID == trID {
			transacao.status = 1
		}
	}
	fmt.Printf("COMMIT DE T%d CONCEDIDO \n", trLabel)
	str := strconv.Itoa(trLabel)
	saida = saida + "C(" + str + ") "

	op_ul(trManager, lockTable, waitFor, grafoEspera, trID, "")
}

/*

	FUNÇÕES PARA LIBERAÇÃO DE BLOQUEIOS E ORGANIZAR LISTAS DE ESPERA (ITENS E TRANSAÇÕES)

*/

func op_ul(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, trID int, idItem string) {
	if idItem != "" {
		liberarBloqueioItem(trManager, lockTable, waitFor, grafoEspera, trID, idItem)
	} else {
		liberarBloqueioTransacao(trManager, lockTable, waitFor, grafoEspera, trID)
	}
}

// Liberação de CURTA duração
func liberarBloqueioItem(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, trID int, idItem string) {
	for idx_bloqueio, bloqueio := range *lockTable {
		if bloqueio.idItem == idItem && bloqueio.trID == trID {
			*lockTable = removerBloqueio(lockTable, idx_bloqueio)

			tipo_bloqueio := getTipoBloqueioString(bloqueio.tipo)
			fmt.Printf("Bloqueio de %s LIBERADO: T%d no item [%s] \n", tipo_bloqueio, bloqueio.trLabel, idItem)
			escalonarWaitFor(trManager, lockTable, waitFor, grafoEspera, bloqueio.idItem)
			break
		}
	}
}

// Liberação via COMMIT
func liberarBloqueioTransacao(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, trID int) {
	for idx_bloqueio := len(*lockTable) - 1; idx_bloqueio >= 0; idx_bloqueio-- {
		bloqueio := (*lockTable)[idx_bloqueio]
		if bloqueio.trID == trID {
			*lockTable = removerBloqueio(lockTable, idx_bloqueio)

			tipo_bloqueio := getTipoBloqueioString(bloqueio.tipo)
			fmt.Printf("Bloqueio de %s LIBERADO: T%d no item [%s] \n", tipo_bloqueio, bloqueio.trLabel, bloqueio.idItem)
			escalonarWaitFor(trManager, lockTable, waitFor, grafoEspera, bloqueio.idItem)
		}
	}
}

func removerBloqueio(lockTable *[]*LockTableItem, idx int) []*LockTableItem {
	if len(*lockTable) <= 1 {
		return (*lockTable)[:0]
	} else if idx == len(*lockTable)-1 {
		return (*lockTable)[:len(*lockTable)-1]
	} else {
		return append((*lockTable)[:idx], (*lockTable)[idx+1:]...)
	}

}

func getTipoBloqueioString(tipo int) string {
	if tipo == 1 {
		return "ESCRITA"
	}
	return "LEITURA"
}

func escalonarWaitFor(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, idItem string) {
	for id_item, wf_item := range *waitFor {
		if wf_item.idItem == idItem {
			if len(wf_item.operacoes) == 0 {
				return
			}

			operacao := wf_item.operacoes[0]
			wf_item.operacoes = wf_item.operacoes[1:]
			if len(wf_item.operacoes) == 0 {
				removeItemEspera(waitFor, id_item)
			}

			if operacao.tipo == 0 {
				executarOperacao(trManager, lockTable, waitFor, grafoEspera, operacao)
				ponteiroParaOperacoes := &wf_item.operacoes
				for _, waitblock := range *ponteiroParaOperacoes {
					if waitblock.tipo == 1 {
						break
					} else if waitblock.tipo == 0 {
						operacao := waitblock
						*ponteiroParaOperacoes = (*ponteiroParaOperacoes)[1:]
						if len(*ponteiroParaOperacoes) == 0 {
							removeItemEspera(waitFor, id_item)
						}
						executarOperacao(trManager, lockTable, waitFor, grafoEspera, operacao)
					}
				}
			} else {
				executarOperacao(trManager, lockTable, waitFor, grafoEspera, operacao)
			}
		}
	}
}

func removeItemEspera(waitFor *[]*WaitForItem, index int) {
	if len(*waitFor) == 1 {
		*waitFor = (*waitFor)[:0]
	} else if index == len(*waitFor)-1 {
		*waitFor = (*waitFor)[:len(*waitFor)-1]
	} else {
		*waitFor = append((*waitFor)[:index], (*waitFor)[index+1:]...)
	}
}

func executarOperacao(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta, operacao *LockTableItem) {
	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID {
			transacao.status = 0

			if operacao.tipo == 0 {
				res_op_rl := op_rl(trManager, lockTable, waitFor, grafoEspera, operacao)
				if res_op_rl > -1 {
					fmt.Printf("Execução do WAIT-DIE\n")
					wait_die(trManager, grafoEspera, waitFor, operacao, res_op_rl, lockTable)
				} else if res_op_rl == -1 {
					executarOperacoesEspera(transacao, trManager, lockTable, waitFor, grafoEspera)
				}
			} else {
				res_op_wl := op_wl(trManager, lockTable, waitFor, grafoEspera, operacao)
				if res_op_wl > -1 {
					fmt.Printf("Execução do WAIT-DIE\n")
					wait_die(trManager, grafoEspera, waitFor, operacao, res_op_wl, lockTable)
				} else if res_op_wl == -1 {
					executarOperacoesEspera(transacao, trManager, lockTable, waitFor, grafoEspera)
				}
			}
		}
	}
}

func executarOperacoesEspera(transacao *TrManagerItem, trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Aresta) {
	for _, block := range transacao.espera {
		if block.tipo == 1 && transacao.status == 0 {
			res_op_wl := op_wl(trManager, lockTable, waitFor, grafoEspera, block)
			if res_op_wl > -1 {
				wait_die(trManager, grafoEspera, waitFor, block, res_op_wl, lockTable)
			}
		} else if block.tipo == 0 && transacao.status == 0 {
			res_op_rl := op_rl(trManager, lockTable, waitFor, grafoEspera, block)
			if res_op_rl > -1 {
				wait_die(trManager, grafoEspera, waitFor, block, res_op_rl, lockTable)
			}
		}
	}
}

/*

	FUNÇÕES PARA RESOLUÇÃO DE CONFLITOS (WAIT-DIE)

*/

// Após verficar que houve conflito, evitar deadlock usando estratégia WAIT-DIE
func wait_die(trManager *[]*TrManagerItem, grafoEspera *[]Aresta, waitFor *[]*WaitForItem, bloqueio *LockTableItem, trIDwaitdie int, lockTable *[]*LockTableItem) Aresta {

	aresta_padrao := Aresta{-1, -1}

	if bloqueio.trID > trIDwaitdie {
		for _, transacao := range *trManager {
			if transacao.trID == bloqueio.trID {
				transacao.status = 2
				for _, block := range *lockTable {
					if block.trLabel == transacao.label {
						op_ul(trManager, lockTable, waitFor, grafoEspera, block.trID, block.idItem)
					}
				}
			}
		}

		trLabelwaitdie := -1
		for _, transacao := range *trManager {
			if transacao.trID == trIDwaitdie {
				trLabelwaitdie = transacao.label
			}
		}

		fmt.Printf("TimeStamp(T%d) > TimesStamp(T%d)\n", bloqueio.trLabel, trLabelwaitdie)
		fmt.Printf("T%d foi ABORTADA => T%d possui o bloqueio sobre o item [%s]", bloqueio.trLabel, trLabelwaitdie, bloqueio.idItem)
		str := strconv.Itoa(bloqueio.trLabel)
		saida = saida + " ABORT" + str + " "
		return aresta_padrao

	} else {
		trLabelwaitdie := -1
		for _, transacao := range *trManager {
			if transacao.trID == trIDwaitdie {
				trLabelwaitdie = transacao.label
			}
		}

		fmt.Printf("TimeStamp(T%d) < TimesStamp(T%d)\n", bloqueio.trLabel, trLabelwaitdie)

		nova_aresta := Aresta{bloqueio.trLabel, trLabelwaitdie}

		*grafoEspera = append(*grafoEspera, nova_aresta)

		for _, aresta := range *grafoEspera {
			if aresta.origem == trLabelwaitdie && aresta.destino == bloqueio.trLabel {
				// DEADLOCK
				fmt.Println("DEADLOCK! Ciclo no grafo de espera identificado.")
				return aresta
			}
		}

		if hasCycles(*grafoEspera) {
			fmt.Println("DEADLOCK! Ciclo no grafo de espera identificado.")
			return aresta_padrao
		}

		fmt.Printf("T%d entra na lista FIFO esperando por liberação do bloqueio de T%d sobre o item [%s] \n", bloqueio.trLabel, trLabelwaitdie, bloqueio.idItem)

		//Transação é colocada como ESPERANDO
		for _, transacao := range *trManager {
			if transacao.trID == bloqueio.trID {
				transacao.status = 3
			}
		}

		//Bloqueio é adicionado na wait for do item
		for _, wf_item := range *waitFor {
			if wf_item.idItem == bloqueio.idItem {
				wf_item.operacoes = append(wf_item.operacoes, bloqueio)
				return aresta_padrao
			}
		}

		//Se não existir item na Wait For List, é criado um e colocadoba Wait For List
		var waitlist []*LockTableItem
		waitlist = append(waitlist, bloqueio)

		wf_item := WaitForItem{
			idItem:    bloqueio.idItem,
			operacoes: waitlist,
		}

		*waitFor = append(*waitFor, &wf_item)

		return aresta_padrao

	}
}

func hasCycles(grafoEspera []Aresta) bool {
	// Nós vamos utilizar uma variável visited para marcar os nós visitados durante a busca
	visited := make(map[int]bool)

	// Nós também vamos usar uma variável recursionStack para marcar os nós presentes na pilha de recursão durante a busca
	recursionStack := make(map[int]bool)

	// Função auxiliar dfs para realizar a busca em profundidade recursiva
	var dfs func(node int) bool
	dfs = func(node int) bool {
		// Marca o nó atual como visitado e adiciona na pilha de recursão
		visited[node] = true
		recursionStack[node] = true

		// Para cada aresta saindo do nó atual
		for _, aresta := range grafoEspera {
			if aresta.origem == node {
				destino := aresta.destino

				// Se o destino não foi visitado, fazemos uma busca recursiva nele
				if !visited[destino] {
					if dfs(destino) {
						return true
					}
				} else if recursionStack[destino] {
					// Se o destino já foi visitado e está na pilha de recursão, então há um ciclo
					return true
				}
			}
		}

		// Quando saímos do nó, removemos ele da pilha de recursão
		recursionStack[node] = false
		return false
	}

	// Percorremos todos os nós do grafo para verificar se há ciclo partindo de cada um deles
	for _, aresta := range grafoEspera {
		if !visited[aresta.origem] {
			if dfs(aresta.origem) {
				return true
			}
		}
	}

	return false
}

/*


/*

	FUNÇÕES AUXILIARES PARA PRINTAR SAÍDAS

*/

func statusParaString(valorNumericoDoStatus int) string {
	switch valorNumericoDoStatus {
	case 0:
		return "ativa"
	case 1:
		return "concluída"
	case 2:
		return "abortada"
	case 3:
		return "esperando"
	default:
		return " "
	}
}

func tipoBloqueioParaString(valorNumericoDoBloqueio int) string {

	switch valorNumericoDoBloqueio {
	case 0:
		return "RL"
	case 1:
		return "WL"
	default:
		return " "
	}
}

func tipoBloqueioExtensoParaString(valorNumericoDoBloqueio int) string {

	switch valorNumericoDoBloqueio {
	case 0:
		return "LEITURA"
	case 1:
		return "ESCRITA"
	default:
		return " "
	}
}

func duracaoBloqueioParaString(valorNumeroDuracao int) string {
	switch valorNumeroDuracao {
	case 0:
		return "Curta"
	case 1:
		return "Longa"
	default:
		return " "
	}
}

func printColoredText(text string, colorCode int) {
	fmt.Printf("\x1b[38;5;%dm%s\x1b[0m", colorCode, text)
}

func printarTrManager(trManager []*TrManagerItem) {
	fmt.Println("LISTA DE TRANSAÇÕES")
	if len(trManager) == 0 {
		fmt.Println("Vazia")
	}
	for _, transacao := range trManager {
		fmt.Printf("T%d - TimeStamp: %d , Status: %s, Bloqueios esperando: { ", transacao.label, transacao.trID, statusParaString(transacao.status))
		quantidade_bloqueios := len(transacao.espera)
		if quantidade_bloqueios == 1 {
			tipoBloqueio := tipoBloqueioParaString((transacao.espera)[0].tipo)
			duracaoBloqueio := duracaoBloqueioParaString((transacao.espera)[0].duracao)
			linha := "" + tipoBloqueio + strconv.Itoa((transacao.espera)[0].trLabel) + "(" + (transacao.espera)[0].idItem + ") - TimeStamp: " + strconv.Itoa((transacao.espera)[0].trID) + ", duração: " + duracaoBloqueio + " "
			fmt.Print(linha)
		} else {
			for ind_bloqueio, bloqueio := range transacao.espera {
				tipoBloqueio := tipoBloqueioParaString((*bloqueio).tipo)
				duracaoBloqueio := duracaoBloqueioParaString((*bloqueio).duracao)
				linha := "" + tipoBloqueio + strconv.Itoa((*bloqueio).trLabel) + "(" + (*bloqueio).idItem + ") - TimeStamp: " + strconv.Itoa((*bloqueio).trID) + ", duração: " + duracaoBloqueio + ""
				if ind_bloqueio == quantidade_bloqueios-1 {
					linha += " "
				} else {
					linha += " | "
				}
				fmt.Print(linha)
			}
		}
		fmt.Println("}")
	}
	fmt.Print("\n")
}

func printLockTable(lockTable []*LockTableItem) {
	fmt.Println("LISTA DE BLOQUEIOS CONCEDIDOS")
	if len(lockTable) == 0 {
		fmt.Print("Vazia\n\n")
	}
	for _, bloqueio := range lockTable {
		linha := "" + tipoBloqueioParaString((*bloqueio).tipo) + strconv.Itoa((*bloqueio).trLabel) + "(" + (*bloqueio).idItem + ") - TimeStamp: " + strconv.Itoa((*bloqueio).trID) + ", duração: " + duracaoBloqueioParaString((*bloqueio).duracao) + "\n"
		fmt.Print(linha)
	}
	fmt.Print("\n")
}

func printarWaitFor(waitFor []*WaitForItem) {
	fmt.Println("LISTA DE ITENS COM BLOQUEIOS EM ESPERA")
	if len(waitFor) > 0 {
		for _, wf_item := range waitFor {
			fmt.Printf("Item [%s] - Bloqueios esperando: { ", (*wf_item).idItem)
			quantidade_bloqueios := len((*wf_item).operacoes)
			for ind, bloqueio := range (*wf_item).operacoes {
				linha := "" + tipoBloqueioParaString((*bloqueio).tipo) + strconv.Itoa((*bloqueio).trLabel) + "(" + (*bloqueio).idItem + ") - TimeStamp: " + strconv.Itoa((*bloqueio).trID) + ", duração: " + duracaoBloqueioParaString((*bloqueio).duracao) + ""
				if ind == quantidade_bloqueios-1 {
					linha += "  "
				} else {
					linha += " | "
				}
				fmt.Print(linha)
			}
			fmt.Println("}")
		}
	} else {
		fmt.Print("Vazia\n\n")
	}
}

func printarGrafo(grafoEspera []Aresta) {
	fmt.Println("ARESTAS DO GRAFO DE ESPERA")
	for _, aresta := range grafoEspera {
		fmt.Printf("Origem: %d - Destino: %d \n\n", aresta.origem, aresta.destino)
	}
	if len(grafoEspera) == 0 {
		fmt.Println("Vazia")
	}
}

/*

	EXECUÇÃO MAIN

*/

// Variável global para guardar o schedule de saída
var saida string

// Variável global relacionada a cor do texto
var corTextoVerde int

func main() {
	corTextoVerde = 10

	//Estruturas de dados
	var trManager []*TrManagerItem
	var lockTable []*LockTableItem
	var waitFor []*WaitForItem
	var grafoEspera []Aresta

	//Variáveis de entrada
	var schedule string
	var isolationLevel int

	// Duração: 0-> curta; 1-> longa.
	var duracao_leitura int
	var duracao_escrita int

	printColoredText("********************************* MENU ****************************************", 9)
	fmt.Println("\n ******************** NIVEL ISOLAMENTO ******************** ")
	fmt.Println("[ 1 ] READ UNCOMMITTED")
	fmt.Println("[ 2 ] READ COMMITTED")
	fmt.Println("[ 3 ] REPEATABLE READ")
	fmt.Println("[ 4 ] SERIALIZABLE")
	fmt.Println("[ 5 ] SAIR")
	fmt.Print("Escolha uma opção: ")
	fmt.Scan(&isolationLevel)

	fmt.Println("\n ******************** ESCALONAMENTO DE ENTRADA ******************** ")
	fmt.Scan(&schedule)

	fmt.Println("\n ******************** INICIO DO ESCALONAMENTO ******************** ")

	//str := "BT(1)r1(x)BT(2)w2(x)r2(y)r1(y)C(1)r2(z)C(2)"
	schedule = strings.ToUpper(schedule)
	operacoes := strings.Split(schedule, ")")
	operacoes = operacoes[:(len(operacoes) - 1)]

	//Atribuindo características a cada nível de isolamento
	if isolationLevel == 1 {
		duracao_escrita = 0
		duracao_leitura = 0
	} else if isolationLevel == 2 {
		duracao_escrita = 1
		duracao_leitura = 0
	} else if isolationLevel == 3 {
		duracao_escrita = 1
		duracao_leitura = 1
	} else if isolationLevel == 4 {
		duracao_escrita = 1
		duracao_leitura = 1
	}

	//Leitura sequencial do schedule de entrada
	for _, operacao := range operacoes {

		//Inicia a transação
		if string(operacao[0]) == "B" {
			label, _ := strconv.Atoi(string(operacao[len(operacao)-1]))
			op_BT(&trManager, label) //função BT
			fmt.Printf("\nA transação %d foi ATIVADA \n", label)

			//Operação de leitura (READ)
		} else if string(operacao[0]) == "R" {
			trLabel, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			//Busca a transação correspondente da leitura solicitada
			for _, transacao := range trManager {
				//se estiver ATIVA, o bloqueio é instanciado
				if transacao.label == trLabel && transacao.status == 0 {
					trID := transacao.trID //timestamp da transação
					bloqueio := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						duracao: duracao_leitura,
						tipo:    0,
					}

					fmt.Printf("\nBloqueio de LEITURA SOLICITADO: T%d no item [%s] \n", trLabel, idItem)
					retorno_read := op_rl(&trManager, &lockTable, &waitFor, &grafoEspera, &bloqueio)
					//se o retorno for um timestamp (dif -1) => houve conflito e precisamos fazer WAIT-DIE
					if retorno_read > -1 {
						wait_die(&trManager, &grafoEspera, &waitFor, &bloqueio, retorno_read, &lockTable)
					}

					//se estiver ESPERANDO, o bloqueio é instanciado
				} else if transacao.label == trLabel && transacao.status == 3 {
					trID := transacao.trID //timestamp da transação
					bloqueio := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						duracao: duracao_leitura,
						tipo:    0,
					}
					//adicionar bloqueio na lista de espera da transação
					transacao.espera = append(transacao.espera, &bloqueio)
					fmt.Printf("Operação de LEITURA de [%s] EM AGUARDO, pois T%d está com status ESPERANDO \n", idItem, trLabel)
					fmt.Println(transacao.espera)
				}
			}

			//Operação de escrita (WRITE)
		} else if string(operacao[0]) == "W" {
			trLabel, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			//Busca a transação correspondente da escrita solicitada
			for _, transacao := range trManager {
				//se estiver ATIVA, o bloqueio é instanciado
				if transacao.label == trLabel && transacao.status == 0 {
					trID := transacao.trID //timestamp da transação
					bloqueio := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						duracao: duracao_escrita,
						tipo:    1,
					}

					fmt.Printf("\nBloqueio de ESCRITA SOLICITADO: T%d no item [%s] \n", trLabel, idItem)
					retorno_write := op_wl(&trManager, &lockTable, &waitFor, &grafoEspera, &bloqueio)
					//se o retorno for um timestamp (dif -1) => houve conflito e precisamos fazer WAIT-DIE
					if retorno_write > -1 {
						wait_die(&trManager, &grafoEspera, &waitFor, &bloqueio, retorno_write, &lockTable)
					}

					//se estiver ESPERANDO, o bloqueio é instanciado
				} else if transacao.label == trLabel && transacao.status == 3 {
					trID := transacao.trID //timestamp da transação
					bloqueio := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						duracao: duracao_escrita,
						tipo:    1,
					}
					//adicionar bloqueio na lista de espera da transação
					transacao.espera = append(transacao.espera, &bloqueio)
					fmt.Printf("Operação de ESCRITA de [%s] EM AGUARDO, pois T%d está com status ESPERANDO \n", idItem, trLabel)
					fmt.Println(transacao.espera)
				}
			}

			//Operação de COMMIT
		} else if string(operacao[0]) == "C" {
			label, _ := strconv.Atoi(string(operacao[len(operacao)-1]))

			//Busca a transação correspondente da escrita solicitada
			for _, transacao := range trManager {
				//se estiver ATIVA, o commit é solicitado
				if transacao.label == label && transacao.status == 0 {

					fmt.Printf("COMMIT DE T%d SOLICITADO \n", label)
					op_C(&trManager, &lockTable, &waitFor, &grafoEspera, transacao.trID, label)

					//se estiver ESPERANDO, o commit é instanciado
				} else if transacao.label == label && transacao.status == 3 {
					commit := LockTableItem{
						idItem:  "",
						trLabel: label,
						trID:    transacao.trID,
						duracao: -1,
						tipo:    -1,
					}
					//adicionar commit na lista de espera da transação

					transacao.espera = append(transacao.espera, &commit)
				}
			}

		}

		printColoredText("\n ---------------------------------- DADOS ---------------------------------- \n", corTextoVerde)
		fmt.Println("*****************************")
		printarTrManager(trManager)
		fmt.Println("*****************************")
		printLockTable(lockTable)
		fmt.Println("*****************************")
		printarWaitFor(waitFor)
		fmt.Println("*****************************")
		printarGrafo(grafoEspera)
		fmt.Println("*****************************")
		printColoredText(" ESCALONAMENTO DA SAIDA DE EXECUCAO: ", corTextoVerde)
		fmt.Println(saida)
		printColoredText("\n ----------------------------------------------------------------------------- \n\n", corTextoVerde)
	}

}
