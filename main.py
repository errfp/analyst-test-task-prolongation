import pandas as pd
import numpy as np

try:
    prolongations_df = pd.read_csv('prolongations.csv', sep=',')
    financial_data_df = pd.read_csv('financial_data.csv', sep=',')
except FileNotFoundError as e:
    print(f"КРИТИЧЕСКАЯ ОШИБКА: Файл не найден. Детали: {e}")
    exit()

if prolongations_df.empty or financial_data_df.empty:
    print("!!! КРИТИЧЕСКАЯ ОШИБКА: Одна из таблиц пуста после загрузки. Проверьте разделитель (sep).")
    exit()
else:
    print("Файлы успешно загружены.")

# Приводим первую букву месяца к заглавной
prolongations_df['month'] = prolongations_df['month'].str.capitalize()

month_columns = [
    'Ноябрь 2022', 'Декабрь 2022', 'Январь 2023', 'Февраль 2023', 'Март 2023', 'Апрель 2023',
    'Май 2023', 'Июнь 2023', 'Июль 2023', 'Август 2023', 'Сентябрь 2023', 'Октябрь 2023',
    'Ноябрь 2023', 'Декабрь 2023'
]

# Очистка чисел
for col in month_columns:
    if col in financial_data_df.columns:
        series = financial_data_df[col].astype(str)
        series = series.str.replace(r'\s', '', regex=True)
        series = series.str.replace(',', '.', regex=False)
        series = series.replace({'в ноль': np.nan, 'стоп': -1, 'end': -1})
        financial_data_df[col] = pd.to_numeric(series, errors='coerce').fillna(0)

# Агрегация дубликатов по id
financial_data_agg = financial_data_df.groupby('id')[month_columns].sum().reset_index()
prolongations_df['id'] = prolongations_df['id'].astype(str).str.strip()
financial_data_agg['id'] = financial_data_agg['id'].astype(str).str.strip()
merged_df = pd.merge(prolongations_df, financial_data_agg, on='id', how='left')

# Фильтрация
month_to_num = {name: i + 1 for i, name in enumerate(month_columns)}
def check_for_stop(row):
    try:
        end_month_idx = month_columns.index(row['month'])
        for i in range(end_month_idx + 1):
            if row[month_columns[i]] == -1: return True
    except (ValueError, KeyError): return False
    return False

merged_df['to_exclude'] = merged_df.apply(check_for_stop, axis=1)
analytics_df = merged_df[~merged_df['to_exclude']].copy()
print(f"Проектов для анализа после всех фильтраций: {len(analytics_df)}")


long_df = analytics_df.melt(id_vars=['id', 'month', 'AM'], value_vars=month_columns, var_name='shipment_month_name', value_name='shipment_sum')
long_df['end_month_num'] = long_df['month'].map(month_to_num)
long_df['shipment_month_num'] = long_df['shipment_month_name'].map(month_to_num)


results_summary = []
results_detailed = []
managers = analytics_df['AM'].dropna().unique()

# Цикл по месяцам 2023 года (начинается с индекса 2 - 'Январь 2023')
for M_idx in range(2, len(month_columns)):
    current_month_num = M_idx + 1 # Текущий месяц
    prev_month_num = M_idx # Прошлый месяц
    prev_2_month_num = M_idx - 1 # Позапрошлый месяц
    
    # K1
    # 1. Находим проекты, завершившиеся в прошлом месяце.
    base_k1_df = long_df[long_df['end_month_num'] == prev_month_num]
    # 2. Считаем их отгрузки за прошлый месяц.
    sum_base_k1 = base_k1_df[base_k1_df['shipment_month_num'] == prev_month_num].groupby('AM')['shipment_sum'].sum()
    # 3. Считаем их отгрузки за текущий месяц.
    sum_prolonged_k1 = base_k1_df[base_k1_df['shipment_month_num'] == current_month_num].groupby('AM')['shipment_sum'].sum()
    # 4. Делим одно на другое
    k1 = (sum_prolonged_k1 / sum_base_k1).fillna(0)
    # K2
    # 1. Находим проекты, завершившиеся два месяца назад.
    base_k2_df = long_df[long_df['end_month_num'] == prev_2_month_num]
    # 2. Исключаем из них те, что уже были продлены в прошлом месяце.
    prolonged_in_M1_ids = base_k2_df[(base_k2_df['shipment_month_num'] == prev_month_num) & (base_k2_df['shipment_sum'] > 0)]['id'].unique()
    base_k2_filtered_df = base_k2_df[~base_k2_df['id'].isin(prolonged_in_M1_ids)]
    # 3. Считаем для оставшихся "упущенных" отгрузки за позапрошлый месяц.
    sum_base_k2 = base_k2_filtered_df[base_k2_filtered_df['shipment_month_num'] == prev_2_month_num].groupby('AM')['shipment_sum'].sum()
    # 4. Считаем отгрузки "упущенных" в текущем месяце.
    sum_prolonged_k2 = base_k2_filtered_df[base_k2_filtered_df['shipment_month_num'] == current_month_num].groupby('AM')['shipment_sum'].sum()
    
    k2 = (sum_prolonged_k2 / sum_base_k2).fillna(0)
    
    month_name = month_columns[M_idx]
    
    for manager in managers:
        results_summary.append({'Месяц': month_name, 'Менеджер': manager, 'Коэффициент_1': k1.get(manager, 0), 'Коэффициент_2': k2.get(manager, 0)})
    total_k1 = sum_prolonged_k1.sum() / sum_base_k1.sum() if sum_base_k1.sum() > 0 else 0
    total_k2 = sum_prolonged_k2.sum() / sum_base_k2.sum() if sum_base_k2.sum() > 0 else 0
    results_summary.append({'Месяц': month_name, 'Менеджер': 'Весь отдел', 'Коэффициент_1': total_k1, 'Коэффициент_2': total_k2})

    base_k1 = base_k1_df[base_k1_df['shipment_month_num'] == prev_month_num][['id', 'AM', 'shipment_sum']].rename(columns={'shipment_sum': 'База'})
    prol_k1 = base_k1_df[base_k1_df['shipment_month_num'] == current_month_num][['id', 'shipment_sum']].rename(columns={'shipment_sum': 'Пролонгация'})
    details_k1 = pd.merge(base_k1, prol_k1, on='id', how='left').fillna(0)
    if not details_k1.empty:
        details_k1.loc[:, 'Месяц_расчета'] = month_name
        details_k1.loc[:, 'Тип_коэф'] = 'K1'
        results_detailed.extend(details_k1.to_dict('records'))
    
    base_k2 = base_k2_filtered_df[base_k2_filtered_df['shipment_month_num'] == prev_2_month_num][['id', 'AM', 'shipment_sum']].rename(columns={'shipment_sum': 'База'})
    prol_k2 = base_k2_filtered_df[base_k2_filtered_df['shipment_month_num'] == current_month_num][['id', 'shipment_sum']].rename(columns={'shipment_sum': 'Пролонгация'})
    details_k2 = pd.merge(base_k2, prol_k2, on='id', how='left').fillna(0)
    if not details_k2.empty:
        details_k2.loc[:, 'Месяц_расчета'] = month_name
        details_k2.loc[:, 'Тип_коэф'] = 'K2'
        results_detailed.extend(details_k2.to_dict('records'))

if not results_detailed:
    print("\nНедостаточно данных для расчета.")
    annual_report_df = pd.DataFrame(columns=['Менеджер', 'Годовой_К1', 'Годовой_К2'])
else:
    detailed_df = pd.DataFrame(results_detailed)

    # Расчет годового К1
    k1_data = detailed_df[detailed_df['Тип_коэф'] == 'K1']
    k1_annual = k1_data.groupby('AM').agg(
        Total_Base=('База', 'sum'),
        Total_Prolongation=('Пролонгация', 'sum')
    ).reset_index()
    k1_annual['Годовой_К1'] = (k1_annual['Total_Prolongation'] / k1_annual['Total_Base']).fillna(0)

    # Расчет годового К2
    k2_data = detailed_df[detailed_df['Тип_коэф'] == 'K2']
    k2_annual = k2_data.groupby('AM').agg(
        Total_Base=('База', 'sum'),
        Total_Prolongation=('Пролонгация', 'sum')
    ).reset_index()
    k2_annual['Годовой_К2'] = (k2_annual['Total_Prolongation'] / k2_annual['Total_Base']).fillna(0)

    # Расчет для всего отдела
    total_k1_base = k1_data['База'].sum()
    total_k1_prol = k1_data['Пролонгация'].sum()
    total_annual_k1 = total_k1_prol / total_k1_base if total_k1_base > 0 else 0

    total_k2_base = k2_data['База'].sum()
    total_k2_prol = k2_data['Пролонгация'].sum()
    total_annual_k2 = total_k2_prol / total_k2_base if total_k2_base > 0 else 0
    annual_report_df = pd.merge(
        k1_annual[['AM', 'Годовой_К1']],
        k2_annual[['AM', 'Годовой_К2']],
        on='AM',
        how='outer'
    ).fillna(0)

    # Добавляем строку "Весь отдел"
    total_row = pd.DataFrame([{'AM': 'Весь отдел', 'Годовой_К1': total_annual_k1, 'Годовой_К2': total_annual_k2}])
    annual_report_df = pd.concat([annual_report_df, total_row], ignore_index=True)
    
    #Cортировка
    manager_order = sorted([m for m in annual_report_df['AM'].unique() if m != 'Весь отдел']) + ['Весь отдел']
    annual_report_df['AM'] = pd.Categorical(annual_report_df['AM'], categories=manager_order, ordered=True)
    annual_report_df = annual_report_df.sort_values('AM').rename(columns={'AM': 'Менеджер'})

summary_df = pd.DataFrame(results_summary)
detailed_report_df = pd.DataFrame(results_detailed)

all_managers = summary_df['Менеджер'].unique()
manager_names = sorted([m for m in all_managers if m not in ['Весь отдел', 'без А/М'] and pd.notna(m)])
custom_order = manager_names + ['без А/М', 'Весь отдел']
summary_df['Менеджер'] = pd.Categorical(summary_df['Менеджер'], categories=custom_order, ordered=True)
summary_df = summary_df.sort_values('Менеджер')

final_report_df = summary_df.pivot_table(index='Менеджер', columns='Месяц', values=['Коэффициент_1', 'Коэффициент_2']).fillna(0)
correct_month_order = [m for m in month_columns if m in final_report_df.columns.get_level_values(1)]
final_report_df = final_report_df.reindex(correct_month_order, axis=1, level=1)

with pd.ExcelWriter('prolongation_report.xlsx') as writer:
    # Добавляем новый лист с годовым отчетом
    annual_report_df.to_excel(writer, sheet_name='Годовой отчет', index=False)
    final_report_df.to_excel(writer, sheet_name='Сводный отчет по месяцам')
    detailed_report_df.to_excel(writer, sheet_name='Детализация по проектам', index=False)
    summary_df.to_excel(writer, sheet_name='Данные для сводного отчета', index=False)

print("\nОтчет успешно сформирован!")