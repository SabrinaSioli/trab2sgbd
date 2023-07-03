package main

import (
	"Scheduler"
	"fmt"
	"strconv"
	"strings"
)

// estrutura que representa TRANSAÇÃO
type TrManagerItem struct {
	label int // nome da transação
	trID  int // timeStamp
	// status: 0-> ativa; 1-> concluída; 2-> abortada; 3-> esperando
	status int
}

// estrutura que representa BLOQUEIO
type LockTableItem struct {
	idItem string //nome do item
	trLabel int //nome da transação
	trID   int    //id da transação
	// duração: 0-> curta; 1-> longa
	duracao int
	// tipo: 0-> leitura; 1-> escrita
	tipo int
}

// estrutura que representa ITEM
type WaitForItem struct {
	idItem    string           //nome do item
	bloqueios []*LockTableItem //lista de bloqueios aguardando
}

type Tupla struct {
	p1 int
	p2 int
}

func op_BT(trManager *[]*TrManagerItem, trID int) {

	transacao := TrManagerItem{
		trID:   trID,
		status: 0,
	}

	*trManager = append(*trManager, &transacao)
}

func op_rl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID && transacao.status == 0 {
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID && bloqueio.tipo == 1 {
					return bloqueio.trID
				}

			}

			fmt.Println(fmt.Sprintf("|| === Transação %d - Obtém bloqueio de Leitura sobre o item %s", operacao.trID, operacao.idItem))

			*lockTable = append(*lockTable, operacao)

			if operacao.duracao == 0 {
				op_ul(trManager, lockTable, waitFor, operacao.trID, operacao.idItem)
			}

			return -1

		}
	}

	return -1

}

func op_wl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID && transacao.status == 0 {
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID {
					return bloqueio.trID
				}

			}

			fmt.Println(fmt.Sprintf("|| === Transação %d - Obtém bloqueio de Escrita sobre o item %s", operacao.trID, operacao.idItem))

			*lockTable = append(*lockTable, operacao)

			if operacao.duracao == 0 {
				op_ul(trManager, lockTable, waitFor, operacao.trID, operacao.idItem)
			}

			return -1

		}
	}

	return -1

}

func op_ul(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, trID int, idItem string) {

	for idx_bloqueio, bloqueio := range *lockTable {
		// fmt.Println(lockTable)
		// fmt.Println(idx_bloqueio, bloqueio)
		if idItem != "" {
			if bloqueio.idItem == idItem && bloqueio.trID == trID {

				*lockTable = append((*lockTable)[:idx_bloqueio], (*lockTable)[idx_bloqueio+1:]...)

				escalonarWaitFor(trManager, lockTable, waitFor, bloqueio.idItem)

				var tipo_bloqueio string
				if bloqueio.tipo == 1 {
					tipo_bloqueio = "Escrita"
				} else {
					tipo_bloqueio = "Leitura"
				}

				fmt.Println(fmt.Sprintf("|| === Transação %d - Libera bloqueio de %s sobre o item %s", trID, tipo_bloqueio, idItem))
			}

		} else {
			if bloqueio.trID == trID {
				// fmt.Println(idx_bloqueio, idx_bloqueio+1, len(*lockTable))
				if len(*lockTable) < 2 {
					*lockTable = (*lockTable)[:0]

				} else if idx_bloqueio+1 > len(*lockTable)-1 {
					*lockTable = (*lockTable)[:len(*lockTable)-1]
				} else {
					*lockTable = append((*lockTable)[:idx_bloqueio], (*lockTable)[idx_bloqueio+1:]...)
				}

				escalonarWaitFor(trManager, lockTable, waitFor, bloqueio.idItem)

			}
		}

	}
}

func op_C(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, trID int) {

	for _, transacao := range *trManager {
		if transacao.trID == trID {
			transacao.status = 1
		}
	}

	op_ul(trManager, lockTable, waitFor, trID, "")
}

func op_wait(trManager *[]*TrManagerItem, waitFor *[]*WaitForItem, operacao *LockTableItem, transacao_detentora int) Tupla {

	tupla_padrao := Tupla{-1, -1}

	if operacao.trID > transacao_detentora {

		for _, transacao := range *trManager {

			if transacao.trID == operacao.trID {
				transacao.status = 2
			}
		}

		fmt.Println(fmt.Sprintf("|| === Transação %d - É abortada devido à estratégia Wait-Die (Transação %d possui o bloqueio sobre o item %s)", operacao.trID, transacao_detentora, operacao.idItem))
		return tupla_padrao
	}

	for _, tupla := range *grafoEspera {
		if tupla.p1 == operacao.trID && tupla.p2 == transacao_detentora {

			// DEADLOCK
			fmt.Println(fmt.Sprintf("|| === Transação %d - Se envolve em um Deadlock com a Transação %d", tupla.p1, tupla.p2))
			return tupla
		}
	}

	nova_tupla := Tupla{transacao_detentora, operacao.trID}

	*grafoEspera = append(*grafoEspera, nova_tupla)

	fmt.Println(fmt.Sprintf("|| === %d - Entra na Fila de Espera pela Liberação do Item %s pela Transação %d", operacao.trID, operacao.idItem, transacao_detentora))

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID {
			transacao.status = 3
		}
	}

	for _, wf_item := range *waitFor {
		if wf_item.idItem == operacao.idItem {
			wf_item.bloqueios = append(wf_item.bloqueios, operacao)
			return tupla_padrao
		}
	}

	var lt []*LockTableItem
	lt = append(lt, operacao)

	wf_item := WaitForItem{
		idItem:    operacao.idItem,
		bloqueios: lt,
	}

	*waitFor = append(*waitFor, &wf_item)

	return tupla_padrao

}

func escalonarWaitFor(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, idItem string) {

	for _, wf_item := range *waitFor {

		if wf_item.idItem == idItem {
			if len(wf_item.bloqueios) < 1 {
				return
			}
			operacao := wf_item.bloqueios[0]
			wf_item.bloqueios = wf_item.bloqueios[1:]

			for _, transacao := range *trManager {
				if transacao.trID == operacao.trID {
					transacao.status = 0
				}
			}

			if operacao.tipo == 1 {
				// fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de Escrita sobre o item %s", trID, idItem))
				res_op_wl := op_wl(trManager, lockTable, waitFor, grafoEspera, operacao)

				if res_op_wl != -1 {
					op_wait(trManager, grafoEspera, waitFor, operacao, res_op_wl)
				}

			} else {
				// fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de Escrita sobre o item %s", trID, idItem))
				res_op_rl := op_rl(trManager, lockTable, waitFor, grafoEspera, operacao)

				if res_op_rl != -1 {
					op_wait(trManager, grafoEspera, waitFor, operacao, res_op_rl)
				}
			}
		}
	}
}

func devolverTextoColorido(text string, color string) string {
	novaString := color + text + color
	return novaString
}

// Status: 0-> ativa; 1-> concluída; 2-> abortada; 3-> esperando.
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
func printTransacoes(trManager []*TrManagerItem) {
	fmt.Println("|| === PRINTANDO TABELA TR MANAGER")
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
	fmt.Println(devolverTextoColorido("||       ID            STATUS     ", "\033[31m"))
	for _, item := range trManager {
		linha := "||       " + strconv.Itoa((*item).trID) + "             " + statusParaString((*item).status) + "    "
		fmt.Println(devolverTextoColorido(linha, "\033[31m"))
	}
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
}

func printBloqueios(lockTable []*LockTableItem) {
	fmt.Println("|| === PRINTANDO TABELA LOCK TABLE")
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
	fmt.Println(devolverTextoColorido("|| ITEM   ID    ESCO   DURA  TIP0 ", "\033[31m"))
	for _, item := range lockTable {
		linha := "|| " + (*item).idItem + "      " + strconv.Itoa((*item).trID) + "      " + strconv.Itoa((*item).escopo) + "     " + strconv.Itoa((*item).duracao) + "     " + strconv.Itoa((*item).tipo)
		fmt.Println(devolverTextoColorido(linha, "\033[31m"))
	}
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
}

func printItens(waitFor []*WaitForItem) {
	fmt.Println("|| === PRINTANDO TABELA WAIT FOR TABLE")
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
	fmt.Println(devolverTextoColorido("|| ID    OPERACAO                 ", "\033[31m"))
	/* indice := 0; */
	if len(waitFor) >= 1 {
		linha := "|| " + (*waitFor[0]).idItem + "     " + (*waitFor[0]).idItem + "      " + strconv.Itoa((*waitFor[0]).bloqueios[0].trID) + "      " + strconv.Itoa((*waitFor[0]).bloqueios[0].escopo) + "     " + strconv.Itoa((*waitFor[0]).bloqueios[0].duracao) + "     " + strconv.Itoa((*waitFor[0]).bloqueios[0].tipo)
		fmt.Println(devolverTextoColorido(linha, "\033[31m"))
		for index, item := range waitFor[1:] {
			linha = "||       " + (*item).idItem + "      " + strconv.Itoa((*item).bloqueios[index].trID) + "      " + strconv.Itoa((*item).bloqueios[index].escopo) + "     " + strconv.Itoa((*item).bloqueios[index].duracao) + "     " + strconv.Itoa((*item).bloqueios[index].tipo)
			fmt.Println(devolverTextoColorido(linha, "\033[31m"))
		}
	}
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
}

func printarGrafo(grafoEspera []Tupla) {
	fmt.Println("|| === PRINTANDO TABELA GRAFO DE ESPERA")
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
	fmt.Println(devolverTextoColorido("||       P1            P2     ", "\033[31m"))
	for _, item := range grafoEspera {
		linha := "||       " + strconv.Itoa(item.p1) + "             " + strconv.Itoa(item.p2) + "    "
		fmt.Println(devolverTextoColorido(linha, "\033[31m"))
	}
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
}

func ReadSchedule() {
	var schedule string
	fmt.Print("Digite um Schedule: ")
	fmt.Scan(&schedule)
}

func main() {

	var trManager []*TrManagerItem //lista de transações
	var lockTable []*LockTableItem //lista de bloqueios concedidos
	var waitFor []*WaitForItem     //lista de itens (que tem sua lista de bloqueios aguardando)
	var grafoEspera []Tupla

	// Nível Isolamento: 1-> read uncommitted, 2-> read committed, 3-> repeatable read, 4-> serializable
	// Duração: 0-> curta; 1-> longa.
	var isolationLevel string
	var duracaoLeitura int
	var duracaoEscrita int
	//var opcao_isolamento int;
	var schedule string

	/* INSERIR NOSSO MENU PERSONALIZADO */

	/* 
	fmt.Println(devolverTextoColorido("|| ===         BEM VINDO AO SISTEMA          ===", "\033[32m"))
	fmt.Println("|| === DIGITE A TRASAÇÃO QUE DESEJA EXECUTAR ===")
	fmt.Print(devolverTextoColorido("|| \\__ : ", "\033[31m"))
	fmt.Scanln(&schedule)
	fmt.Println(devolverTextoColorido("|| === DIGITE O NÍVEL DE ISOLAMENTO          ===", "\033[32m"))
	fmt.Print(devolverTextoColorido("|| \\__ : ", "\033[31m"))
	fmt.Scanln(&nivel)
	fmt.Println(devolverTextoColorido("|| ===         INICIANDO OPERAÇÕES           ===", "\033[32m")) 
	*/
	
	for {
		fmt.Println("\n ******************** NIVEL ISOLAMENTO ******************** ")
		fmt.Println("[ 1 ] READ UNCOMMITTED")
		fmt.Println("[ 2 ] READ COMMITTED")
		fmt.Println("[ 3 ] REPEATABLE READ")
		fmt.Println("[ 4 ] SERIALIZABLE")
		fmt.Println("[ 5 ] SAIR")
		fmt.Print("Escolha uma opção: ")
		fmt.Scan(&isolationLevel)

		switch isolationLevel {
		case "1":
		fmt.Println(" _____________ READ UNCOMMITTED _____________ ")
		schedule := ReadSchedule()
		case "2": 
		fmt.Println(" _____________ READ COMMITTED _____________ ")
		schedule := ReadSchedule()
		case "3": 
		fmt.Println(" _____________ REPEATABLE READ _____________ ")
		schedule := ReadSchedule()
		case "4":
		fmt.Println(" _____________ SERIALIZABLE _____________ ")
		schedule := ReadSchedule()

		scheduler := Scheduler.newScheduler(schedule, isolationLevel)
		scheduler.Escalonador(trManager, locktable, waitFor, grafoEspera)

		fmt.Printf("Escalonamento final: " scheduler.ScheduleOut)

		case "5":
			fmt.Println("Saindo do programa...")
			return
		default:
			fmt.Println("Entrada inválida.")
			isolationLevel = "0"
		} 
	

	//str := "BT(1)r1(x)BT(2)w2(x)r2(y)r1(y)C(1)r2(z)C(2)"
	schedule = strings.ToUpper(schedule)
	operacoes := strings.Split(schedule, ")")
	operacoes = operacoes[:(len(operacoes) - 1)]

	if isolationLevel == 0 {
		duracaoEscrita = 0
		duracaoLeitura = 0
	} else if isolationLevel == 1 {
		duracaoEscrita = 1
		duracaoLeitura = 0
	} else if isolationLevel == 2 {
		duracaoEscrita = 1
		duracaoLeitura = 1
	} else if isolationLevel == 3 {
		duracaoEscrita = 1
		duracaoLeitura = 1
	}

	for _, operacao := range operacoes {

		if string(operacao[0]) == "B" {
			trID, _ := strconv.Atoi(string(operacao[len(operacao)-1]))

			fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Começa", "\033[33m"), trID))
			op_BT(&trManager, trID)

			fmt.Println()

		} else if string(operacao[0]) == "W" {
			trID, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.trID == trID && transacao.status != 2 {
					operacao := LockTableItem{
						idItem:  idItem,
						trID:    trID,
						duracao: duracaoEscrita,
						tipo:    1,
					}

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita bloqueio de Escrita sobre o item %s", "\033[33m"), trID, idItem))
					res_op_wl := op_wl(&trManager, &lockTable, &waitFor, &grafoEspera, &operacao)

					if res_op_wl != -1 {
						op_wait(&trManager, &grafoEspera, &waitFor, &operacao, res_op_wl)
					}

					fmt.Println()
				}
			}

		} else if string(operacao[0]) == "R" {
			trID, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.trID == trID && transacao.status != 2 {

					operacao := LockTableItem{
						idItem:  idItem,
						trID:    trID,
						escopo:  0,
						duracao: duracaoLeitura,
						tipo:    0,
					}

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita bloqueio de Leitura sobre o item %s", "\033[33m"), trID, idItem))
					res_op_rl := op_rl(&trManager, &lockTable, &waitFor, &grafoEspera, &operacao)

					if res_op_rl != -1 {
						op_wait(&trManager, &grafoEspera, &waitFor, &operacao, res_op_rl)
					}

					fmt.Println()
				}
			}

		} else if string(operacao[0]) == "C" {
			trID, _ := strconv.Atoi(string(operacao[len(operacao)-1]))

			for _, transacao := range trManager {

				if transacao.trID == trID && transacao.status != 2 {

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita Commit", "\033[33m"), trID))
					op_C(&trManager, &lockTable, &waitFor, &grafoEspera, trID)

					fmt.Println()
				}
			}

		}

		printTransacoes(trManager)
		printBloqueios(lockTable)
		printItens(waitFor)
		//printarGrafo(grafoEspera)
	}

}
