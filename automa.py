import time
import json
from playwright.sync_api import sync_playwright
import psycopg2

print("Lendo protocolos.json...")
with open('protocolos.json', 'r') as arquivo:
    conteudo = json.load(arquivo)
    protocolos = conteudo['protocolos']
    print(f"Carregado {len(protocolos)} protocolos")

# --- INÍCIO DO MOTOR DO BANCO DE DADOS ---
print("Conectando ao banco de dados PostgreSQL...")
conn = psycopg2.connect(
    host="82.29.61.156",
    database="postgres",
    user="webassist",
    password="&q#96I7Sb"
)
cursor = conn.cursor()
print("Banco conectado com sucesso!")
# -----------------------------------------

for protocolo in protocolos:
    print(f"\n[+] - Iniciando por protocolo: {protocolo}")

    # VERIFICAÇÃO PRÉVIA NO BANCO DE DADOS (SKIP)
    cursor.execute("SELECT 1 FROM situacao.evento WHERE protocolo = %s", (int(protocolo),))
    if cursor.fetchone():
        print(f"Protocolo {protocolo} já existe no banco. Pulando ...")
        continue

    # SE NÃO EXISTE, PREPARA PARA RODAR A AUTOMAÇÃO COM RESILIÊNCIA
    sucesso_neste_protocolo = False
    
    while not sucesso_neste_protocolo:
        try:
            print(f"Buscando protocolo {protocolo} no SGA...")
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                page = browser.new_page()
                page.set_viewport_size({"width": 1920, "height": 1080})
                
                # Tolerância padrão estendida
                page.set_default_timeout(120000)
                page.set_default_navigation_timeout(120000)

                page.goto("https://sga.hinova.com.br/sga/sgav4_ancore/v5/login.php")
                time.sleep(2) # Respiro inicial

                try:
                    page.locator("text=Continuar e Fechar").click(timeout=10000)
                except:
                    pass

                # Login
                page.locator("#usuario").fill("patrik dias")

                senha_input = page.locator("#senha")
                senha_input.click()
                senha_input.fill("")
                senha_input.type("Arx5050", delay=100)

                page.keyboard.press("Enter")
                time.sleep(2) # Respiro pós-enter
                page.locator("button[type='submit']").wait_for(state="attached")
                page.locator("button[type='submit']").click(force=True)

                # Fechar Modal
                try:
                    # 1. Garantia matemática que o login concluiu o carregamento
                    try:
                        page.locator("#navbar8").wait_for(state="visible", timeout=25000)
                    except:
                        pass

                    # 2. Respiro leve de frontend pro modal começar a brotar (caso o site esteja engasgando)
                    time.sleep(1)
                    
                    modal = page.locator("#myModal")
                    
                    try:
                        # NOTA: timeout=8000 no playwright NÃO trava seu código por 8 seg fixos!
                        # Se o modal aparecer no 1º segundo, ele avança imediatamente. 8s é só o limite máximo elástico!
                        modal.wait_for(state="visible", timeout=8000)
                        print("[+] Modal emergente detectado na tela! Iniciando exclusão...")
                    except:
                        pass
                    
                    tentativas_modal = 0
                    # Força bruta: fica tentando clicar e fechar enquanto ele ainda estiver visível!
                    while True:
                        if not modal.is_visible():
                            print("[+] Ameaça eliminada! Tela limpa de modais e backdrops.")
                            break
                            
                        print(f"[*] Tentando fechamento do modal... (Tentativa {tentativas_modal+1}) - Loop infinito até fechar")
                        try:
                            modal.get_by_role("button", name="Fechar").click(force=True, timeout=2000)
                        except:
                            try:
                                modal.get_by_role("button", name="Close").click(force=True, timeout=2000)
                            except:
                                page.keyboard.press("Escape")
                                
                        # Pequeno paliativo apenas para desbloquear o fundo escuro do modal se ele agarrar
                        try:
                            page.evaluate("""
                                document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
                                document.body.classList.remove('modal-open');
                            """)
                        except:
                            pass
                            
                        time.sleep(1)
                        tentativas_modal += 1
                except Exception as erro_modal:
                    print(f"[-] Log do modal (ignorado): {erro_modal}")

                # Navegação
                time.sleep(3) # Respiro longo pro site estabilizar home
                page.locator("#navbar8").click()
                time.sleep(1) # Respiro pro dropdown
                page.locator("a.dropdown-item:has-text('9.3) Consultar Evento')").click()

                time.sleep(3) # Respiro longo pra página consultar abrir
                page.locator("text=Todos").click()
                time.sleep(1)
                
                protocolo_info = page.locator("#protocolo")
                protocolo_info.type(str(protocolo))
                time.sleep(1)
                page.keyboard.press("Enter")

                # Espera da engrenagem do Hinova
                try:
                    page.locator("#load").wait_for(state="visible", timeout=3000)
                except:
                    pass
                page.locator("#load").wait_for(state="hidden", timeout=120000)
                time.sleep(2) # Respiro de precaução final após grid carregar

                encontrou = False
                tentativas = 0
                max_tentativas = 20

                while not encontrou and tentativas < max_tentativas:
                    linhas = page.locator("tr")
                    total_linhas = linhas.count()

                    for i in range(total_linhas):
                        texto_linha = linhas.nth(i).inner_text().lower()
                        # Verifica se o número do protocolo e a palavra 'associado' estão na linha
                        if str(protocolo).lower() in texto_linha and "associado" in texto_linha:
                            encontrou = True
                            print(f"Protocolo {protocolo} (Associado) encontrado na linha {i}!")
                            
                            linhas.nth(i).locator("a.btn-primary.dropdown").click()
                            time.sleep(1) # respiro do dropdown
                            
                            with page.context.expect_page() as nova_aba_info:
                                linhas.nth(i).locator("a.dropdown-item:has-text('Editar Evento')").click()
                            
                            nova_aba = nova_aba_info.value
                            nova_aba.wait_for_load_state()
                            time.sleep(3) # respiro importante pra aba nova renderizar full

                            nova_aba.locator("button[data-target='#historicosEventoVeiculo']").click()
                            time.sleep(3) # Respiro da expansão do accordion
                            
                            # Lista com variações e proteções contra erros de ortografia do sistema
                            palavras_chave = [
                                "dados da analise", "dados da análise",
                                "dados de analise", "dados de análise",
                                "parecer juridico", "parecer jurídico",
                                "conclusao", "conclusão",
                                "possibilidades"
                            ]
                            
                            # Transforma a lista de forma dinâmica no seletor múltiplo (isto OU aquilo OU aquele)
                            seletor_multiplo = ", ".join([f"td:has-text('{p}')" for p in palavras_chave])
                            td_dados = nova_aba.locator(seletor_multiplo).first
                            
                            texto_extraido = "Sem informacao do juridico." # Fallback Padrão

                            try:
                                print("[*] Inspecionando a tela em busca de Pareceres, Conclusões ou Dados da Análise...")
                                td_dados.wait_for(state="visible", timeout=10000)
                                texto_extraido = td_dados.inner_text()
                                print(f"[+] Informação encontrada e extraída com sucesso!")
                            except Exception:
                                print(f"ℹ Nenhuma das opções da lista foi detectada para este protocolo. Usando mensagem padrão.")
                            
                            # Realiza a gravação no banco independente se extraiu ou usou fallback
                            try:
                                query = """
                                    INSERT INTO situacao.evento (protocolo, dados_analise)
                                    VALUES (%s, %s);
                                """
                                cursor.execute(query, (int(protocolo), texto_extraido))
                                conn.commit()
                                print(f"[OK] Protocolo {protocolo} salvo no banco com sucesso!")

                            except Exception as db_e:
                                print(f"[ERRO] Erro fatal de banco de dados ao tentar salvar protocolo: {db_e}")
                            
                            break  # Sai do FOR interno

                    if not encontrou:
                        tentativas += 1
                        if tentativas < max_tentativas:
                            print(f"Protocolo não encontrado (Tentativa {tentativas}/{max_tentativas}). Enter...")
                            time.sleep(1)
                            page.keyboard.press("Enter")
                            try:
                                page.locator("#load").wait_for(state="visible", timeout=3000)
                            except:
                                pass
                            page.locator("#load").wait_for(state="hidden", timeout=120000)
                            time.sleep(2)
                        else:
                            print(f"[-] Protocolo {protocolo} não apareceu após {max_tentativas} tentativas. Gravando no BD como não encontrado.")
                            
                            try:
                                query = """
                                    INSERT INTO situacao.evento (protocolo, dados_analise)
                                    VALUES (%s, %s);
                                """
                                cursor.execute(query, (int(protocolo), "Não encontrado."))
                                conn.commit()
                                print(f"[OK] Protocolo {protocolo} gravado no banco como 'Não encontrado.'!")
                            except Exception as db_e:
                                print(f"[ERRO] Falha ao tentar salvar ausência no banco: {db_e}")
                
                # Tudo certo, marca como sucesso pra fechar o browser e pular pro próximo da lista!
                sucesso_neste_protocolo = True
                print(f"Encerrando ciclo do browser para {protocolo} de forma limpa. . .")
                
        except Exception as excecao:
            # Capturou Timeouts no site da hinova travado, não cracha o script, reseta!
            print(f"\n[!] O site engasgou ou a rede travou! Erro capturado: {excecao}")
            print("[+] Reiniciando o Chrome para tentar ESSE MESMO protocolo do zero...\n")
            time.sleep(3) # Pausa de alívio pra rede

# Desligando o motor do banco de dados de maneira segura após o término
cursor.close()
conn.close()
print("\n [FIM] Extração em Lote Finalizada! Banco Desconectado com sucesso.")