import time
import json
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.set_viewport_size({"width": 1920, "height": 1080})

    page.goto("https://sga.hinova.com.br/sga/sgav4_ancore/v5/login.php")

    try:
        page.locator("text=Continuar e Fechar").click(timeout=10000)
        print("Pop-Up inicial fechado")
    except:
        print("Pop-Up inicial não encontrado, continuando")

    # Login
    page.locator("#usuario").fill("patrik dias")

    senha_input = page.locator("#senha")
    senha_input.click()
    senha_input.fill("")
    senha_input.type("Arx5050", delay=100)

    page.keyboard.press("Enter")
    print("Senha digitada e Enter pressionado")

    page.locator("button[type='submit']").wait_for(state="attached")
    time.sleep(1)
    page.locator("button[type='submit']").click(force=True)
    print("Clicando no Entrar")

    try:
        # Aguarda o modal ficar visível
        page.wait_for_selector("#myModal", state="visible", timeout=10000)

        # Tenta clicar no botão "Fechar" do modal-footer
        page.locator("#myModal .modal-footer button").click(timeout=5000)
        print("Modal pós-login fechado pelo footer")

    except:
        try:
            # Fallback: clica pelo texto
            page.get_by_text("Fechar").click(timeout=5000)
            print("Modal fechado pelo texto 'Fechar'")
        except:
            try:
                # Fallback: botão X do header
                page.locator("#myModal .modal-header .close").click(timeout=5000)
                print("Modal fechado pelo X")
            except:
                print("Modal pós-login não encontrado, continuando")


    page.locator("#navbar8").click()
    page.locator("a.dropdown-item:has-text('9.3) Consultar Evento')").click()

    with open('protocolos.json', 'r') as arquivo:
        conteudo = json.load(arquivo)
        protocolos = conteudo['protocolos']
        
        print(f"Carregado {len(protocolos)} protocolos")

    for protoloco in protocolos:
        print(f"[+] - Iniciando por protocolo: {protoloco}")

        page.locator("text=Todos").click()
        time.sleep(10)
        protocolo_info = page.locator("#protocolo")
        protocolo_info.type(str(protoloco))
        page.keyboard.press("Enter")


        time.sleep(500)
        


    input("Digite Enter para fechar o browser")
    browser.close()
