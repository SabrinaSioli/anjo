package main

import (
	"fmt"
	"strconv"
	"strings"
)

type TrManagerItem struct {
	label int // nome da transação
	trID  int
	// Status: 0-> ativa; 1-> concluída; 2-> abortada; 3-> esperando.
	status int
	espera []*LockTableItem
}

type LockTableItem struct {
	idItem  string
	trLabel int //nome da transação
	trID    int
	// Escopo: 0-> objeto; 1-> predicado.
	escopo int
	// Duração: 0-> curta; 1-> longa.
	duracao int
	// Tipo: 0-> leitura; 1-> escrita.
	tipo int
}

type WaitForItem struct {
	idItem    string
	operacoes []*LockTableItem
}

type Tupla struct {
	p1 int
	p2 int
}

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

func op_rl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID && transacao.status == 0 {
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID && bloqueio.tipo == 1 {
					return bloqueio.trID
				}

			}

			fmt.Println(fmt.Sprintf("|| === Transação %d (ts: %d)- Obtém bloqueio de Leitura sobre o item %s", operacao.trLabel, operacao.trID, operacao.idItem))
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

func op_wl(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID && transacao.status == 0 {
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID {
					return bloqueio.trID
				}

			}

			fmt.Println(fmt.Sprintf("|| === Transação %d - Obtém bloqueio de Escrita sobre o item %s", operacao.trLabel, operacao.idItem))
			str := strconv.Itoa(operacao.trLabel)
			saida = saida + "W" + str + "(" + operacao.idItem + ") "
			*lockTable = append(*lockTable, operacao)

			if operacao.duracao == 0 {
				op_ul(trManager, lockTable, waitFor, grafoEspera, operacao.trID, operacao.idItem)
			}

			return -1

		} else if transacao.trID == operacao.trID && transacao.status == 1 {
			//perguntar
		}
	}

	return -1

}

func op_ul(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, trID int, idItem string) {

	for idx_bloqueio, bloqueio := range *lockTable {
		// fmt.Println(lockTable)
		// fmt.Println(idx_bloqueio, bloqueio)
		if idItem != "" {
			if bloqueio.idItem == idItem && bloqueio.trID == trID {

				*lockTable = append((*lockTable)[:idx_bloqueio], (*lockTable)[idx_bloqueio+1:]...)

				var tipo_bloqueio string
				if bloqueio.tipo == 1 {
					tipo_bloqueio = "Escrita"
				} else {
					tipo_bloqueio = "Leitura"
				}

				fmt.Println(fmt.Sprintf("|| === Transação %d - Libera bloqueio de %s sobre o item %s", bloqueio.trLabel, tipo_bloqueio, idItem))

				escalonarWaitFor(trManager, lockTable, waitFor, grafoEspera, bloqueio.idItem)
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

				escalonarWaitFor(trManager, lockTable, waitFor, grafoEspera, bloqueio.idItem)

			}
		}

	}
}

func op_C(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, trID int, trLabel int) {

	for _, transacao := range *trManager {
		if transacao.trID == trID {
			transacao.status = 1
		}
	}

	str := strconv.Itoa(trLabel)
	saida = saida + "C(" + str + ") "

	op_ul(trManager, lockTable, waitFor, grafoEspera, trID, "")
}

func op_wait(trManager *[]*TrManagerItem, grafoEspera *[]Tupla, waitFor *[]*WaitForItem, operacao *LockTableItem, transacao_detentora int) Tupla {

	tupla_padrao := Tupla{-1, -1}

	if operacao.trID > transacao_detentora {

		for _, transacao := range *trManager {

			if transacao.trID == operacao.trID {
				transacao.status = 2
			}
		}

		fmt.Println(fmt.Sprintf("|| === Transação %d - É abortada devido à estratégia Wait-Die (Transação %d possui o bloqueio sobre o item %s)", operacao.trID, transacao_detentora, operacao.idItem))
		str := strconv.Itoa(operacao.trLabel)
		saida = saida + " Abort" + str + " "
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
			wf_item.operacoes = append(wf_item.operacoes, operacao)
			return tupla_padrao
		}
	}

	var lt []*LockTableItem
	lt = append(lt, operacao)

	wf_item := WaitForItem{
		idItem:    operacao.idItem,
		operacoes: lt,
	}

	*waitFor = append(*waitFor, &wf_item)

	return tupla_padrao

}

func escalonarWaitFor(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, idItem string) {

	for id_item, wf_item := range *waitFor {

		if wf_item.idItem == idItem {
			if len(wf_item.operacoes) < 1 {
				return
			}
			operacao := wf_item.operacoes[0]
			wf_item.operacoes = wf_item.operacoes[1:]
			if len(wf_item.operacoes) == 0 {
				if len(*waitFor) == 1 {
					*waitFor = (*waitFor)[:0]
				} else if id_item == len(*waitFor)-1 {
					*waitFor = (*waitFor)[:len(*waitFor)-1]
				} else {
					*waitFor = append((*waitFor)[:id_item], (*waitFor)[id_item+1:]...)
				}
			}

			for _, transacao := range *trManager {
				if transacao.trID == operacao.trID {
					transacao.status = 0

					if operacao.tipo == 1 {
						// fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de Escrita sobre o item %s", trID, idItem))
						res_op_wl := op_wl(trManager, lockTable, waitFor, grafoEspera, operacao)
						if res_op_wl != -1 {
							op_wait(trManager, grafoEspera, waitFor, operacao, res_op_wl)
						} else {
							for idblock, block := range transacao.espera {
								if block.tipo == 1 && transacao.status == 0 {
									res_op_wl := op_wl(trManager, lockTable, waitFor, grafoEspera, block)
									if res_op_wl != -1 {
										op_wait(trManager, grafoEspera, waitFor, block, res_op_wl)
										break
									}

									//removendo da lista de espera da transação
									if len(transacao.espera) == 1 {
										transacao.espera = (transacao.espera)[:0]
									} else if id_item == len(transacao.espera)-1 {
										transacao.espera = (transacao.espera)[:len(transacao.espera)-1]
									} else {
										transacao.espera = append((transacao.espera)[:idblock], (transacao.espera)[idblock+1:]...)
									}

								} else if block.tipo == 0 && transacao.status == 0 {
									res_op_rl := op_rl(trManager, lockTable, waitFor, grafoEspera, block)
									if res_op_rl != -1 {
										op_wait(trManager, grafoEspera, waitFor, block, res_op_rl)
										break
									}

									//removendo da lista de espera da transação
									if len(transacao.espera) == 1 {
										transacao.espera = (transacao.espera)[:0]
									} else if id_item == len(transacao.espera)-1 {
										transacao.espera = (transacao.espera)[:len(transacao.espera)-1]
									} else {
										transacao.espera = append((transacao.espera)[:idblock], (transacao.espera)[idblock+1:]...)
									}
								}
							}
						}

					} else if operacao.tipo == 0 {
						// fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de Escrita sobre o item %s", trID, idItem))
						res_op_rl := op_rl(trManager, lockTable, waitFor, grafoEspera, operacao)

						if res_op_rl != -1 {
							op_wait(trManager, grafoEspera, waitFor, operacao, res_op_rl)
						} else {
							for _, block := range transacao.espera {
								if block.tipo == 1 && transacao.status == 0 {
									res_op_wl := op_wl(trManager, lockTable, waitFor, grafoEspera, block)
									if res_op_wl != -1 {
										op_wait(trManager, grafoEspera, waitFor, block, res_op_wl)
									}
								} else if block.tipo == 0 && transacao.status == 0 {
									res_op_rl := op_rl(trManager, lockTable, waitFor, grafoEspera, block)
									if res_op_rl != -1 {
										op_wait(trManager, grafoEspera, waitFor, block, res_op_rl)
									}
								}
							}
						}
					}
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
func printarTrManager(trManager []*TrManagerItem) {
	fmt.Println("|| === PRINTANDO TABELA TR MANAGER")
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
	fmt.Println(devolverTextoColorido("||      LABEL          TS         STATUS     ", "\033[31m"))
	for _, item := range trManager {
		linha := "||      " + strconv.Itoa((*item).label) + "              " + strconv.Itoa((*item).trID) + "        " + statusParaString((*item).status)
		fmt.Println(devolverTextoColorido(linha, "\033[31m"))
	}
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
}

func printLockTable(lockTable []*LockTableItem) {
	fmt.Println("|| === PRINTANDO TABELA LOCK TABLE")
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
	fmt.Println(devolverTextoColorido("|| ITEM   LABEL    TS    ESCO   DURA  TIP0 ", "\033[31m"))
	for _, item := range lockTable {
		linha := "|| " + (*item).idItem + "         " + strconv.Itoa((*item).trLabel) + "      " + strconv.Itoa((*item).trID) + "      " + strconv.Itoa((*item).escopo) + "     " + strconv.Itoa((*item).duracao) + "     " + strconv.Itoa((*item).tipo)
		fmt.Println(devolverTextoColorido(linha, "\033[31m"))
	}
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
}

func printarWaitFor(waitFor []*WaitForItem) {
	fmt.Println("|| === PRINTANDO TABELA WAIT FOR TABLE")
	fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"))
	fmt.Println(devolverTextoColorido("|| ID    OPERACAO                 ", "\033[31m"))
	/* indice := 0; */
	if len(waitFor) >= 1 {
		linha := "|| " + (*waitFor[0]).idItem + "     " + (*waitFor[0]).idItem + "      " + strconv.Itoa((*waitFor[0]).operacoes[0].trID) + "      " + strconv.Itoa((*waitFor[0]).operacoes[0].escopo) + "     " + strconv.Itoa((*waitFor[0]).operacoes[0].duracao) + "     " + strconv.Itoa((*waitFor[0]).operacoes[0].tipo)
		fmt.Println(devolverTextoColorido(linha, "\033[31m"))
		for index, item := range waitFor[1:] {
			linha = "||       " + (*item).idItem + "      " + strconv.Itoa((*item).operacoes[index].trID) + "      " + strconv.Itoa((*item).operacoes[index].escopo) + "     " + strconv.Itoa((*item).operacoes[index].duracao) + "     " + strconv.Itoa((*item).operacoes[index].tipo)
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

var saida string

func main() {

	var trManager []*TrManagerItem
	var lockTable []*LockTableItem
	var waitFor []*WaitForItem
	var grafoEspera []Tupla
	// Nível Isolamento: 0-> read uncommitted, 1-> read committed, 2-> repeatable read, 3-> serializable.
	var nivel_isolamento int
	// Duração: 0-> curta; 1-> longa.
	var duracao_leitura int
	var duracao_escrita int

	//var opcao_isolamento int;
	var str string

	fmt.Println(devolverTextoColorido("|| ===         BEM VINDO AO SISTEMA          ===", "\033[32m"))
	fmt.Println("|| === DIGITE A TRASAÇÃO QUE DESEJA EXECUTAR ===")
	fmt.Print(devolverTextoColorido("|| \\__ : ", "\033[31m"))
	fmt.Scanln(&str)
	fmt.Println(devolverTextoColorido("|| === DIGITE O NÍVEL DE ISOLAMENTO          ===", "\033[32m"))
	fmt.Print(devolverTextoColorido("|| \\__ : ", "\033[31m"))
	fmt.Scanln(&nivel_isolamento)
	fmt.Println(devolverTextoColorido("|| ===         INICIANDO OPERAÇÕES           ===", "\033[32m"))

	//str := "BT(1)r1(x)BT(2)w2(x)r2(y)r1(y)C(1)r2(z)C(2)"
	str = strings.ToUpper(str)

	partes := strings.Split(str, ")")
	partes = partes[:(len(partes) - 1)]

	//nivel_isolamento = 3

	if nivel_isolamento == 0 {
		duracao_escrita = 0
		duracao_leitura = 0

	} else if nivel_isolamento == 1 {
		duracao_escrita = 1
		duracao_leitura = 0

	} else if nivel_isolamento == 2 {
		duracao_escrita = 1
		duracao_leitura = 1
	} else {
		duracao_escrita = 1
		duracao_leitura = 1
	}

	for _, operacao := range partes {

		if string(operacao[0]) == "B" {
			label, _ := strconv.Atoi(string(operacao[len(operacao)-1]))

			fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Começa", "\033[33m"), label))
			op_BT(&trManager, label)

			fmt.Println()

		} else if string(operacao[0]) == "W" {
			trLabel, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.label == trLabel && transacao.status == 0 {
					trID := transacao.trID //timestamp da transação
					operacao := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						escopo:  0,
						duracao: duracao_escrita,
						tipo:    1,
					}

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita bloqueio de Escrita sobre o item %s", "\033[33m"), transacao.label, idItem))
					res_op_wl := op_wl(&trManager, &lockTable, &waitFor, &grafoEspera, &operacao)

					if res_op_wl != -1 {
						op_wait(&trManager, &grafoEspera, &waitFor, &operacao, res_op_wl)
					}

					fmt.Println()
				} else if transacao.label == trLabel && transacao.status == 3 {
					trID := transacao.trID //timestamp da transação
					operacao := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						escopo:  0,
						duracao: duracao_escrita,
						tipo:    0,
					}
					transacao.espera = append(transacao.espera, &operacao)
				}
			}

		} else if string(operacao[0]) == "R" {
			trLabel, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.label == trLabel && transacao.status == 0 {
					trID := transacao.trID //timestamp da transação
					operacao := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						escopo:  0,
						duracao: duracao_leitura,
						tipo:    0,
					}

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita bloqueio de Leitura sobre o item %s", "\033[33m"), trLabel, idItem))
					res_op_rl := op_rl(&trManager, &lockTable, &waitFor, &grafoEspera, &operacao)

					if res_op_rl != -1 {
						op_wait(&trManager, &grafoEspera, &waitFor, &operacao, res_op_rl)
					}

					fmt.Println()
				} else if transacao.label == trLabel && transacao.status == 3 {
					trID := transacao.trID //timestamp da transação
					operacao := LockTableItem{
						idItem:  idItem,
						trLabel: trLabel,
						trID:    trID,
						escopo:  0,
						duracao: duracao_leitura,
						tipo:    0,
					}
					transacao.espera = append(transacao.espera, &operacao)
					fmt.Println("leitura pulada")
					fmt.Println(transacao.espera)
				}
			}

		} else if string(operacao[0]) == "C" {
			label, _ := strconv.Atoi(string(operacao[len(operacao)-1]))

			for _, transacao := range trManager {

				if transacao.label == label && transacao.status == 0 {

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita Commit", "\033[33m"), label))
					op_C(&trManager, &lockTable, &waitFor, &grafoEspera, transacao.trID, label)

					fmt.Println()
				} else if transacao.label == label && transacao.status == 3 {

					operacao := LockTableItem{
						idItem:  "",
						trLabel: label,
						trID:    transacao.trID,
						escopo:  -1,
						duracao: -1,
						tipo:    -1,
					}
					transacao.espera = append(transacao.espera, &operacao)
				}
			}

		}

		printarTrManager(trManager)
		printLockTable(lockTable)
		printarWaitFor(waitFor)
		printarGrafo(grafoEspera)
		fmt.Println(saida)
	}

}
