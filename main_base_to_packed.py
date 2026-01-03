import asyncio
from playwright.async_api import async_playwright
import time
import datetime
import os
import shutil
import pandas as pd
import gspread
# SUBSTITUÍDO: oauth2client (obsoleto) por google.oauth2 (moderno)
from google.oauth2.service_account import Credentials 
import zipfile
import gc
import traceback # Para ver o erro real

DOWNLOAD_DIR = "/tmp/shopee_automation"

def rename_downloaded_file(download_dir, download_path):
    """Renames the downloaded file to include the current hour."""
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"TO-Packed{current_hour}.zip"
        new_file_path = os.path.join(download_dir, new_file_name)
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None

def unzip_and_process_data(zip_path, extract_to_dir):
    try:
        unzip_folder = os.path.join(extract_to_dir, "extracted_files")
        os.makedirs(unzip_folder, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_folder)
        print(f"Arquivo '{os.path.basename(zip_path)}' descompactado.")

        csv_files = [os.path.join(unzip_folder, f) for f in os.listdir(unzip_folder) if f.lower().endswith('.csv')]
        
        if not csv_files:
            print("Nenhum arquivo CSV encontrado no ZIP.")
            shutil.rmtree(unzip_folder)
            return None

        print(f"Lendo e unificando {len(csv_files)} arquivos CSV...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        # === INÍCIO DA LÓGICA DE PROCESSAMENTO ===
        print("Iniciando processamento dos dados...")
        
        print("Aplicando filtro: SoC_SP_Cravinhos...")
        if not df_final.empty:
            df_final = df_final[df_final.iloc[:, 12] == "SoC_SP_Cravinhos"]
            print(f"Linhas restantes após filtro: {len(df_final)}")

        colunas_desejadas = [0, 9, 15, 17, 2, 23]
        df_selecionado = df_final.iloc[:, colunas_desejadas].copy()
        
        df_selecionado.columns = ['Chave', 'Coluna9', 'Coluna15', 'Coluna17', 'Coluna2', 'Coluna23']

        contagem = df_selecionado['Chave'].value_counts().reset_index()
        contagem.columns = ['Chave', 'Quantidade']

        agrupado = df_selecionado.groupby('Chave').agg({
            'Coluna9': 'first',
            'Coluna15': 'first',
            'Coluna17': 'first',
            'Coluna2': 'first',
            'Coluna23': 'first',
        }).reset_index()

        resultado = pd.merge(agrupado, contagem, on='Chave')
        resultado = resultado[['Chave', 'Coluna9', 'Coluna15', 'Coluna17', 'Quantidade', 'Coluna2', 'Coluna23']]
        
        print(f"Processamento concluído. DataFrame final tem {len(resultado)} linhas.")
        shutil.rmtree(unzip_folder)
        return resultado
        
    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        return None

def update_google_sheet_with_dataframe(df_to_upload):
    """Updates a Google Sheet using native gspread methods and modern auth."""
    if df_to_upload is None or df_to_upload.empty:
        print("Nenhum dado para enviar.")
        return
        
    try:
        print(f"Preparando envio de {len(df_to_upload)} linhas para o Google Sheets...")
        
        # --- AUTENTICAÇÃO MODERNA (Google Auth) ---
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Verifica se o arquivo existe antes de tentar carregar
        if not os.path.exists("hxh.json"):
            raise FileNotFoundError("O arquivo 'hxh.json' não foi encontrado no diretório atual.")

        creds = Credentials.from_service_account_file("hxh.json", scopes=scope)
        client = gspread.authorize(creds)
        
        planilha = client.open("Stage Out Management - SP5 - SPX")
        aba = planilha.worksheet("Packed")
        
        # 1. Limpar a aba
        print("Limpando a aba 'Packed'...")
        aba.clear() 
        
        # 2. Enviar Cabeçalho (Manualmente, sem usar gspread_dataframe)
        headers = df_to_upload.columns.tolist()
        aba.append_rows([headers], value_input_option='USER_ENTERED')
        
        # 3. Preparar dados
        df_to_upload = df_to_upload.fillna('')
        dados_lista = df_to_upload.values.tolist()
        
        chunk_size = 2000 # Reduzi um pouco por segurança
        total_chunks = (len(dados_lista) // chunk_size) + 1
        
        print(f"Iniciando upload de {len(dados_lista)} registros em {total_chunks} lotes...")

        for i in range(0, len(dados_lista), chunk_size):
            chunk = dados_lista[i:i + chunk_size]
            aba.append_rows(chunk, value_input_option='USER_ENTERED')
            print(f" -> Lote {i//chunk_size + 1}/{total_chunks} enviado.")
            time.sleep(2) 
        
        print("✅ SUCESSO! Dados enviados para o Google Sheets.")
        time.sleep(2)

    except Exception as e:
        print("❌ ERRO CRÍTICO NO UPLOAD:")
        print(f"Mensagem de erro: {str(e)}")
        print("Traceback completo:")
        traceback.print_exc() # Isso vai mostrar EXATAMENTE onde o código quebrou

async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    async with async_playwright() as p:
        # Configuração do browser para evitar detecção e melhorar performance
        browser = await p.chromium.launch(
            headless=False, 
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            # LOGIN
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill('Ops71223')
            await page.locator('xpath=//*[@placeholder="Senha"]').fill('@Shopee123')
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_timeout(10000)
            
            # Tentar fechar popup se existir
            try:
                if await page.locator('.ssc-dialog-close').is_visible():
                    await page.locator('.ssc-dialog-close').click()
            except:
                pass
            
            # NAVEGAÇÃO
            await page.goto("https://spx.shopee.com.br/#/general-to-management")
            await page.wait_for_timeout(8000)
            
            # Exportar
            await page.get_by_role('button', name='Exportar').click()
            await page.wait_for_timeout(5000)
            await page.locator('xpath=/html[1]/body[1]/span[4]/div[1]/div[1]/div[1]').click()
            await page.wait_for_timeout(5000)
            await page.get_by_role("treeitem", name="Packed", exact=True).click()
            await page.wait_for_timeout(5000)
            await page.get_by_role("button", name="Confirmar").click()
            
            # Espera estendida para geração do relatório
            await page.wait_for_timeout(60000) 
            
            # DOWNLOAD
            async with page.expect_download(timeout=120000) as download_info:
                await page.get_by_role("button", name="Baixar").first.click()
            
            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)
            print(f"Download concluído: {download_path}")

            # PROCESSAMENTO
            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)
            
            if renamed_zip_path:
                final_dataframe = unzip_and_process_data(renamed_zip_path, DOWNLOAD_DIR)
                update_google_sheet_with_dataframe(final_dataframe)
                
                if final_dataframe is not None:
                    del final_dataframe
                    gc.collect()

        except Exception as e:
            print(f"Erro durante a execução do Playwright: {e}")
            traceback.print_exc()
        finally:
            await browser.close()
            if os.path.exists(DOWNLOAD_DIR):
                shutil.rmtree(DOWNLOAD_DIR)
                print("Limpeza concluída.")

if __name__ == "__main__":
    asyncio.run(main())
