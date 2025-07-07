import streamlit as st
import pandas as pd
import asyncio
import aiohttp 
from io import BytesIO 
from urllib.parse import urlparse, urljoin 
from typing import Any, Dict, List, Optional, Set, Tuple 
import html 
import time 
import traceback 

st.set_page_config(page_title="Проверка URL изображений аптек", page_icon="⚕️", layout="wide")

# --- КОНСТАНТЫ ---
ID_COLUMN_NAME_EXCEL = "id"
URL_TEMPLATES = [
    "https://tmcu.lll.org.ua/pharmacy_properties_api/files/pharmacies/{ID}/imageApteka.jpeg",
    "https://tmcu.lll.org.ua/pharmacy_properties_api/files/pharmacies/{ID}/imageApteka.png"
]
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
# --- КОНЕЦ КОНСТАНТ ---

async def fetch_url_status(
    http_session: aiohttp.ClientSession, 
    pharmacy_id: Any, 
    url_to_check: str, 
    original_extension_type: str, 
    semaphore: asyncio.Semaphore
) -> Dict:
    async with semaphore:
        generated_url = url_to_check
        final_url = "" 
        status_code = None
        status_message = "Неизвестная ошибка"
        error_details = ""
        headers = {'User-Agent': DEFAULT_USER_AGENT}
        try:
            async with http_session.get(url_to_check, headers=headers, timeout=aiohttp.ClientTimeout(total=20), ssl=False) as response:
                final_url = str(response.url)
                status_code = response.status
                if 200 <= status_code < 300: status_message = "ОК" if generated_url == final_url else "Редирект ОК"
                elif status_code == 404: status_message = "Не найдено (404)"
                elif status_code == 403: status_message = "Запрещено (403)"
                elif 400 <= status_code < 500: status_message = f"Ошибка клиента ({status_code})"
                elif 500 <= status_code < 600: status_message = f"Ошибка сервера ({status_code})"
                else: status_message = f"Другой статус ({status_code})"
        except asyncio.TimeoutError: status_message = "Таймаут"; error_details = "Запрос > 20 сек"; final_url = generated_url 
        except aiohttp.ClientConnectorError as e: status_message = "Ошибка соединения"; error_details = str(e); final_url = generated_url
        except aiohttp.ClientError as e: status_message = "Ошибка клиента (aiohttp)"; error_details = str(e); final_url = generated_url
        except Exception as e: status_message = "Непредвиденная ошибка"; error_details = str(e); final_url = generated_url
        await asyncio.sleep(0.05) 
        return {
            "ID аптеки (исходный)": pharmacy_id, 
            "Тип файла (проверенный)": original_extension_type.upper(),
            "Проверяемый URL": generated_url,
            "Конечный URL проверки": final_url,
            "HTTP Статус проверки": status_code,
            "Результат проверки URL": status_message,
            "Детали ошибки URL": error_details
        }

async def run_all_checks_async(
    pharmacy_ids_with_raw_values: list, 
    url_templates_list: List[str], 
    max_concurrent: int, 
    progress_bar_ui, 
    status_text_ui
) -> List[Dict]:
    all_individual_check_results = []
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(ssl=False) 
    async with aiohttp.ClientSession(connector=connector) as http_session:
        tasks = []
        skipped_ids_info_for_direct_add = [] 
        for pharmacy_id_raw, cleaned_id_str_or_none in pharmacy_ids_with_raw_values:
            if cleaned_id_str_or_none is None:
                # Этот ID невалиден, создаем для него "заглушку" в all_individual_check_results,
                # но НЕ создаем для него задач в tasks.
                # Важно: pharmacy_id_raw здесь может быть None (pd.isna), NaN или пустой строкой.
                # Для ключа словаря лучше использовать его строковое представление или ID по умолчанию.
                actual_id_for_report = str(pharmacy_id_raw) if not pd.isna(pharmacy_id_raw) else "ПУСТОЙ_ИЛИ_NAN_ID"
                
                # Добавляем результат для КАЖДОГО шаблона, чтобы логика агрегации не ломалась
                for template_idx, template in enumerate(url_templates_list):
                    ext_type = "JPEG" if ".jpeg" in template.lower() else "PNG" if ".png" in template.lower() else "UNKNOWN"
                    all_individual_check_results.append({
                        "ID аптеки (исходный)": actual_id_for_report,
                        "Тип файла (проверенный)": ext_type, # Указываем тип, хотя проверка не будет
                        "Проверяемый URL": "N/A (пустой или невалидный ID)", 
                        "Конечный URL проверки": "N/A", 
                        "HTTP Статус проверки": None, 
                        "Результат проверки URL": "Пропущено (ID невалиден)", 
                        "Детали ошибки URL": ""
                    })
                continue

            for template in url_templates_list:
                url_to_check = template.replace("{ID}", cleaned_id_str_or_none)
                extension_type = "JPEG" if ".jpeg" in template.lower() else "PNG" if ".png" in template.lower() else "UNKNOWN"
                tasks.append(fetch_url_status(http_session, pharmacy_id_raw, url_to_check, extension_type, semaphore))
        
        total_tasks_to_run = len(tasks)
        processed_tasks_count = 0
        if progress_bar_ui: progress_bar_ui.progress(0.0)
        if status_text_ui: status_text_ui.text(f"Обработано URL: 0/{total_tasks_to_run} (Всего ID для проверки: {len(pharmacy_ids_with_raw_values)})")

        for i, future in enumerate(asyncio.as_completed(tasks)):
            result_item = None
            try:
                result_item = await future
                all_individual_check_results.append(result_item)
            except Exception as e_task:
                print(f"Критическая ошибка при выполнении задачи fetch_url_status: {e_task}")
                # Здесь pharmacy_id из result_item не будет доступен, если await future упал.
                # Это маловероятно, т.к. fetch_url_status ловит Exception.
                # Но если все же упадет, нужен способ идентифицировать задачу.
                all_individual_check_results.append({
                    "ID аптеки (исходный)": f"Ошибка асинхр. задачи (неизвестный ID)", 
                    "Тип файла (проверенный)": "ERROR",
                    "Проверяемый URL": "N/A", "Конечный URL проверки": "N/A", 
                    "HTTP Статус проверки": None, "Результат проверки URL": "Ошибка выполнения задачи", 
                    "Детали ошибки URL": str(e_task)
                })
            
            processed_tasks_count +=1
            if progress_bar_ui: progress_bar_ui.progress(processed_tasks_count / total_tasks_to_run if total_tasks_to_run > 0 else 0)
            if status_text_ui: 
                id_disp = result_item.get('ID аптеки (исходный)', f'задача {i+1}') if result_item else f'задача {i+1} (ошибка)'
                status_text_ui.text(f"Проверка URL: {processed_tasks_count}/{total_tasks_to_run} (ID: {id_disp})")
    
    final_aggregated_results = []
    results_by_pharmacy_id = {}
    for res_item in all_individual_check_results:
        pid_raw = res_item.get("ID аптеки (исходный)")
        # Для группировки используем строковое представление исходного ID, если это не "специальная" строка ошибки
        pid_group_key = str(pid_raw) if not (isinstance(pid_raw, str) and "Ошибка асинхр. задачи" in pid_raw) else f"ERROR_TASK_{time.time()}"

        if pid_group_key not in results_by_pharmacy_id:
            results_by_pharmacy_id[pid_group_key] = {'raw_id': pid_raw, 'checks': []}
        results_by_pharmacy_id[pid_group_key]['checks'].append(res_item)

    for pharmacy_id_key, data in results_by_pharmacy_id.items():
        pharmacy_id_original = data['raw_id']
        checks_for_id = data['checks']

        # Если это была запись о пропущенном/невалидном ID, берем первую (и единственную) запись
        if len(checks_for_id) == 1 and "Пропущено" in checks_for_id[0].get("Результат проверки URL", ""):
            final_aggregated_results.append({
                "ID аптеки": pharmacy_id_original,
                "Сгенерированный URL (основной)": checks_for_id[0].get("Проверяемый URL", "N/A"),
                "Результат": checks_for_id[0].get("Результат проверки URL", "Ошибка данных"),
                "Найденный формат": checks_for_id[0].get("Тип файла (проверенный)", "N/A"),
                "Конечный URL (найденного)": checks_for_id[0].get("Конечный URL проверки", "N/A"),
                "HTTP Статус (конечный)": checks_for_id[0].get("HTTP Статус проверки", "N/A"),
                "Детали по ошибкам (если есть)": checks_for_id[0].get("Детали ошибки URL", "")
            })
            continue
        
        jpeg_check = next((c for c in checks_for_id if c.get("Тип файла (проверенный)") == "JPEG"), None)
        png_check = next((c for c in checks_for_id if c.get("Тип файла (проверенный)") == "PNG"), None)

        is_jpeg_ok = jpeg_check and "ОК" in jpeg_check.get("Результат проверки URL", "")
        is_png_ok = png_check and "ОК" in png_check.get("Результат проверки URL", "")

        aggregated_status = "❌ Не найдено (в обоих форматах)"
        found_format_details = "Нет"
        final_url_display = "N/A"
        http_status_display = "N/A"
        error_details_parts = []
        
        # Формируем "Сгенерированный URL (основной)" на основе первого шаблона и оригинального ID
        # (даже если ID оказался невалидным для формирования URL, это поле будет информативным)
        base_id_str_for_template = str(pharmacy_id_original)
        if base_id_str_for_template.endswith(".0"): base_id_str_for_template = base_id_str_for_template[:-2]
        
        generated_url_template_ref = "N/A (ошибка ID)"
        if base_id_str_for_template and not pd.isna(pharmacy_id_original) and "{ID}" in url_templates_list[0] :
            generated_url_template_ref = url_templates_list[0].replace("{ID}", base_id_str_for_template)


        if is_jpeg_ok and is_png_ok:
            aggregated_status = "✅ ОК"; found_format_details = "JPEG и PNG"
            final_url_display = f"JPEG: {jpeg_check['Конечный URL проверки']}" 
            http_status_display = jpeg_check['HTTP Статус проверки']
        elif is_jpeg_ok:
            aggregated_status = "✅ ОК"; found_format_details = "JPEG"
            final_url_display = jpeg_check['Конечный URL проверки']
            http_status_display = jpeg_check['HTTP Статус проверки']
        elif is_png_ok:
            aggregated_status = "✅ ОК"; found_format_details = "PNG"
            final_url_display = png_check['Конечный URL проверки']
            http_status_display = png_check['HTTP Статус проверки']
        else: 
            if jpeg_check: error_details_parts.append(f"JPEG: {jpeg_check.get('Результат проверки URL','N/A')} ({jpeg_check.get('Проверяемый URL') or generated_url_template_ref})")
            else: error_details_parts.append(f"JPEG: Проверка не проводилась или ошибка ({generated_url_template_ref})")
            
            png_template_url_ref = "N/A (ошибка ID)"
            if base_id_str_for_template and not pd.isna(pharmacy_id_original) and len(url_templates_list) > 1 and "{ID}" in url_templates_list[1]:
                png_template_url_ref = url_templates_list[1].replace("{ID}", base_id_str_for_template)

            if png_check: error_details_parts.append(f"PNG: {png_check.get('Результат проверки URL','N/A')} ({png_check.get('Проверяемый URL') or png_template_url_ref})")
            else: error_details_parts.append(f"PNG: Проверка не проводилась или ошибка ({png_template_url_ref})")
            
            if final_url_display == "N/A": final_url_display = jpeg_check.get('Конечный URL проверки', generated_url_template_ref) if jpeg_check else generated_url_template_ref
            if http_status_display == "N/A": http_status_display = jpeg_check.get('HTTP Статус проверки', "N/A") if jpeg_check else "N/A"

        final_aggregated_results.append({
            "ID аптеки": pharmacy_id_original,
            "Результат": aggregated_status,
            "Найденный формат": found_format_details,
            "Сгенерированный URL (JPEG вариант)": generated_url_template_ref,
            "Конечный URL (если найден)": final_url_display,
            "HTTP Статус (конечный)": http_status_display,
            "Детали по вариантам/ошибкам": "; ".join(error_details_parts) if error_details_parts else ""
        })
            
    return final_aggregated_results

# --- UI Streamlit ---
st.title("⚕️ Проверка доступности изображений аптек (JPEG и PNG)")
st.markdown(f"Утилита для массовой проверки URL изображений по ID аптек. Проверяются два варианта: с расширением `.jpeg` и `.png`.")

uploaded_file = st.file_uploader(
    f"1. Загрузите Excel-файл. Файл должен содержать один столбец с именем **'{ID_COLUMN_NAME_EXCEL}'**, содержащий ID аптек.", 
    type=["xlsx", "xls"]
)

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file, dtype={ID_COLUMN_NAME_EXCEL: str}) 
        st.success(f"Файл '{uploaded_file.name}' успешно загружен. Обнаружено строк: {len(df)}")
        st.markdown("---")
        st.subheader("Предпросмотр данных (первые 5 строк):")
        st.dataframe(df.head())

        if ID_COLUMN_NAME_EXCEL not in df.columns:
            st.error(f"Ошибка: В загруженном файле отсутствует обязательный столбец с именем '{ID_COLUMN_NAME_EXCEL}'.")
        else:
            st.markdown("---")
            st.subheader("2. Настройки проверки")
            max_concurrent_requests = st.slider(
                "Количество параллельных запросов:",
                min_value=1, max_value=30, value=10, 
                help="Определяет, сколько URL будет проверяться одновременно."
            )
            if st.button("🚀 Начать проверку!", key="start_check_button"):
                ids_to_check_raw = df[ID_COLUMN_NAME_EXCEL].tolist()
                pharmacy_ids_with_raw_values_for_run = []
                for id_val in ids_to_check_raw:
                    cleaned_id = None
                    if not pd.isna(id_val):
                        id_str = str(id_val).strip()
                        if id_str.endswith(".0"): id_str = id_str[:-2]
                        if id_str: cleaned_id = id_str
                    pharmacy_ids_with_raw_values_for_run.append((id_val, cleaned_id))
                valid_ids_for_run_count = sum(1 for _, cleaned_id in pharmacy_ids_with_raw_values_for_run if cleaned_id is not None)
                if not valid_ids_for_run_count:
                    st.warning("Не найдено валидных ID для проверки в файле.")
                else:
                    st.info(f"Начинается проверка для {len(pharmacy_ids_with_raw_values_for_run)} записей ({valid_ids_for_run_count} ID будут проверены по {len(URL_TEMPLATES)} шаблонам)...")
                    progress_bar_ui = st.progress(0.0)
                    status_text_ui = st.empty()
                    status_text_ui.text("Инициализация...")
                    all_results_aggregated = []
                    try:
                        all_results_aggregated = asyncio.run(
                            run_all_checks_async(pharmacy_ids_with_raw_values_for_run, URL_TEMPLATES, max_concurrent_requests, progress_bar_ui, status_text_ui)
                        )
                    except Exception as e_async_run:
                         st.error(f"Ошибка при выполнении асинхронных задач: {e_async_run}")
                         st.exception(e_async_run)
                    status_text_ui.success(f"Проверка завершена! Обработано записей: {len(all_results_aggregated)}.")
                    if all_results_aggregated:
                        results_df = pd.DataFrame(all_results_aggregated)
                        cols_order = ["ID аптеки", "Результат", "Найденный формат", "Конечный URL (если найден)", "HTTP Статус (конечный)", "Сгенерированный URL (JPEG вариант)", "Детали по вариантам/ошибкам"]
                        final_cols = [col for col in cols_order if col in results_df.columns] # Показываем только существующие колонки
                        results_df_display = results_df[final_cols]
                        st.markdown("---"); st.subheader("📊 Результаты проверки")
                        st.dataframe(results_df_display)
                        output_excel = BytesIO()
                        with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
                            results_df_display.to_excel(writer, index=False, sheet_name='Результаты_проверки_URL')
                            worksheet = writer.sheets['Результаты_проверки_URL']
                            for idx, col in enumerate(results_df_display): 
                                series = results_df_display[col]
                                max_len = max((series.astype(str).map(len).max(), len(str(series.name)) )) + 2 
                                worksheet.set_column(idx, idx, max_len) 
                        excel_data_to_download = output_excel.getvalue()
                        st.download_button(label="📥 Скачать отчет в Excel", data=excel_data_to_download,
                            file_name=f"отчет_URL_изображений_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    else: st.warning("Результатов для отображения нет.")
    except ValueError as ve: 
        st.error(f"Ошибка чтения данных из Excel: {ve}. Убедитесь, что файл корректен и столбец '{ID_COLUMN_NAME_EXCEL}' содержит ожидаемые данные.")
    except Exception as e:
        st.error(f"Произошла ошибка при обработке файла: {e}")
        st.exception(e)