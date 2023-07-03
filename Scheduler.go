package Scheduler

type SchedulerImpl struct {
	Schedule  string
	Operacoes *[]string
	Nivel     int
}

func newScheduler(schedule string, nivel int) {
	schedule = strings.ToUpper(schedule)
	operacoes := *[]string
	operacoes = strings.Split(schedule, ")")
	operacoes = operacoes[:(len(operacoes) - 1)]

	return &SchedulerImpl{
		Schedule:  schedule,
		Operacoes: operacoes,
		Nivel:     nivel,
	}
}

func Escalonador(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla) {

	//selecionando nível de isolamento
	if nivel == 0 { //read uncommitted
		duracaoEscrita = 0
		duracaoLeitura = 0
	} else if nivel == 1 { //read committed
		duracaoEscrita = 1
		duracaoLeitura = 0
	} else if nivel == 2 { //repeatable read
		duracaoEscrita = 1
		duracaoLeitura = 1
	} else if nivel == 3 { //serializable
		duracaoEscrita = 1
		duracaoLeitura = 1
	}

	//percorrendo operações do schedule de entrada
	for _, operacao := range Operacoes {

		//se operação do tipo BT
		if string(operacao[0]) == "B" { 
			label, _ := strconv.Atoi(string(operacao[len(operacao)-1])) //pegar label da transação
			fmt.Printf("Transação %d foi ativada \n", label)
			op_BT(&trManager, label)
		} 
		//se operação do tipo WRITE
		else if string(operacao[0]) == "W" { 
			label, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.label == label && transacao.status != 2 { //se a transação não tiver abortada, gera o bloqueio
					trID := transacao.trID //timestamp da transação
					bloqueio := LockTableItem{
						idItem:  idItem, //nome item
						trLabel:   label, //nome transação
						trID:    trID, //timestamp
						duracao: duracaoEscrita, //longa ou curta
						tipo:    1, //escrita
					}
					
					fmt.Sprintf("Transação <label= %d, TS = %d, status=%d > : Solicita bloqueio de escrita para item %s", label, trID, transacao.status, idItem)
					
					solicitaW := op_wl(&trManager, &lockTable, &waitFor, &grafoEspera, &bloqueio)

					//se houve conflito verificamos com WAIT DIE
					if solicitaW != -1 {
						op_wait(&trManager, &grafoEspera, &waitFor, &bloqueio, solicitaW)
					}
				}
			}
		} 
		//se operação do tipo READ
		else if string(operacao[0]) == "R" {
			label, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.label == label && transacao.status != 2 { //se transação não for abortada
					bloqueio := LockTableItem{
						idItem:  idItem, //nome item
						trLabel: label, //timestamp transação
						trID:    trID, //nome transação
						duracao: duracaoLeitura, //longa ou curta
						tipo:    0, //ativa
					}

					fmt.Sprintf("Transação <label= %d, TS = %d, status=%d > : Solicita bloqueio de leitura para item %s", label, trID, transacao.status, idItem)
					solicitaR := op_rl(&trManager, &lockTable, &waitFor, &grafoEspera, &bloqueio)

					//se houve conflito verificamos com WAIT DIE
					if solicitaR != -1 {
						op_wait(&trManager, &grafoEspera, &waitFor, &bloqueio, solicitaR)
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
	}
}

//solicitação de ativar transação BT
func op_BT(trManager *[]*TrManagerItem, label int) {

	trID = len(trManager) //timestamp

	transacao := TrManagerItem{
		label:  label, //nome da transação
		trID:   trID,  //timestamp
		status: 0,     //ativa
	}

	*trManager = append(*trManager, &transacao)
}

//solicitação de bloqueio de escrita W
func op_wl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, bloqueio *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == bloqueio.trID && transacao.status == 0 { //se transação for ativa

			//verifica na lista de bloqueios concedidos se há algum conflito
			for _, block := range *lockTable {
				if bloqueio.idItem == block.idItem && bloqueio.trID != block.trID {
					return block.trID //houve conflito, retorna timestamp do bloqueio que impede
				}
			}

			//bloqueio concedido e adicionado na lista locktable
			fmt.Println(fmt.Sprintf("Transação %d - Obtém bloqueio de Escrita sobre o item %s", bloqueio.label, bloqueio.idItem))
			*lockTable = append(*lockTable, bloqueio)

			//se duração for curta, bloqueio já é liberado
			if bloqueio.duracao == 0 {
				op_ul(trManager, lockTable, waitFor, bloqueio.trID, bloqueio.idItem)
			}

			return -1 //retorno de bloqueio concedido

		}
	}

	return -1
}

//solicita bloqueio de leitura
func op_rl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, bloqueio *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == bloqueio.trID && transacao.status == 0 { //se transação for ativa
			
			//verifica na lista de bloqueios concedidos se há algum conflito
			for _, block := range *lockTable {
				if block.idItem == bloqueio.idItem && block.trID != bloqueio.trID && block.tipo == 1 { // se há na lista de bloqueios um bloqueio de transação diferente no mesmo item, sendo do tipo de escrito => gera conflito
					return block.trID //houve conflito, retorna timestamp do bloqueio que impede
				}

			}

			//bloqueio concedido e adicionado na lista locktable
			fmt.Println(fmt.Sprintf("|| === Transação %d - Obtém bloqueio de Leitura sobre o item %s", bloqueio.trID, bloqueio.idItem))
			*lockTable = append(*lockTable, bloqueio)

			//se duração for curta, bloqueio já é liberado
			if operacao.duracao == 0 {
				op_ul(trManager, lockTable, waitFor, bloqueio.trID, bloqueio.idItem)
			}

			return -1 //retorno de bloqueio concedido

		}
	}
	return -1
}

//commita transação
func op_C(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, trID int) {

	//busca por transação a ser commitada
	for _, transacao := range *trManager {
		if transacao.trID == trID {
			transacao.status = 1 //seta status para concluida
		}
	}

	//libera bloqueios da transação
	op_ul(trManager, lockTable, waitFor, trID, "")
}

//libera bloqueio 
func op_ul(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, trID int, idItem string) {

	for ind, block := range *lockTable {
		//liberando bloqueios de curta duração
		if idItem != "" {
			if block.idItem == idItem && block.trID == trID {

				//remover bloqueio da lista de bloqueios concedidos
				*lockTable = append((*lockTable)[:idx_block], (*lockTable)[idx_block+1:]...)

				//verifica lista de espera de bloqueios do item liberado
				escalonarWaitFor(trManager, lockTable, waitFor, block.idItem)

				var printTipo string
				if block.tipo == 1 {
					printTipo = "escrita"
				} else {
					printTipo = "leitura"
				}

				fmt.Println("Transação %d - Libera bloqueio de %s sobre o item %s", block.label, printTipo, idItem)
			}
		} 
		//liberando bloqueios de longa duração (em commit ou abort)
		else {
			//liberar todos os bloqueios de locktable da transação commitada ou abortada
			if block.trID == trID {
				if len(*lockTable) < 2 { //0 ou 1 elementos na lista
					*lockTable = (*lockTable)[:0]
				} else if ind == (len(*lockTable)-1) { //último elemento da lista
					*lockTable = (*lockTable)[:len(*lockTable)-1]
				} else {
					*lockTable = append((*lockTable)[:ind], (*lockTable)[ind+1:]...)
				}

				//verifica lista de espera de bloqueios dos itens liberados
				escalonarWaitFor(trManager, lockTable, waitFor, grafoEspera, block.idItem)
			}
		}

	}
}

func escalonarWaitFor(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, idItem string) {

	for _, item := range *waitFor {

		if item.idItem == idItem {
			// se não tiver bloqueios da lista WaitFor, então sai da função
			if len(item.bloqueios) < 1 {
				return
			}

			bloqueio := item.bloqueios[0] //primeiro bloqueio da lista FIFO
			item.bloqueios = item.bloqueios[1:] //elimina o primeiro bloqueio

			//ativa a transação que estava em espera
			for _, transacao := range *trManager {
				if transacao.trID == bloqueio.trID {
					transacao.status = 0
				}
			}

			if bloqueio.tipo == 1 {
				fmt.Println("Transação %d - Solicita bloqueio de escrita sobre o item %s", trID, idItem)
				solicitaW := op_wl(trManager, lockTable, waitFor, grafoEspera, bloqueio)

				//houve conflito => WAIT DIE
				if solicitaW != -1 {
					op_wait(trManager, grafoEspera, waitFor, bloqueio, solicitaW)
				}

			} else {
				fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de leitura sobre o item %s", trID, idItem))
				solicitaR := op_rl(trManager, lockTable, waitFor, grafoEspera, bloqueio)

				//houve conflito => WAIT DIE
				if solicitaR != -1 {
					op_wait(trManager, grafoEspera, waitFor, bloqueio, solicitaR)
				}
			}
		}
	}
}

func escalonarWaitForCommit(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla) {

	for _, item := range *waitFor {

		if item.idItem == idItem {
			// se não tiver bloqueios da lista WaitFor, então sai da função
			if len(item.bloqueios) < 1 {
				return
			}

			bloqueio := item.bloqueios[0] //primeiro bloqueio da lista FIFO
			item.bloqueios = item.bloqueios[1:] //elimina o primeiro bloqueio

			//ativa a transação que estava em espera
			for _, transacao := range *trManager {
				if transacao.trID == bloqueio.trID {
					transacao.status = 0
				}
			}

			if bloqueio.tipo == 1 {
				fmt.Println("Transação %d - Solicita bloqueio de escrita sobre o item %s", trID, idItem)
				solicitaW := op_wl(trManager, lockTable, waitFor, grafoEspera, bloqueio)

				//houve conflito => WAIT DIE
				if solicitaW != -1 {
					op_wait(trManager, grafoEspera, waitFor, bloqueio, solicitaW)
				}

			} else {
				fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de leitura sobre o item %s", trID, idItem))
				solicitaR := op_rl(trManager, lockTable, waitFor, grafoEspera, bloqueio)

				//houve conflito => WAIT DIE
				if solicitaR != -1 {
					op_wait(trManager, grafoEspera, waitFor, bloqueio, solicitaR)
				}
			}
		}
	}
}