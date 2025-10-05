<div align="center">

# 📦 Warehouse Capacity Script

### Автоматизированный анализ и контроль складских мощностей

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=for-the-badge&logo=google-sheets&logoColor=white)](https://www.google.com/sheets/about/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

*От 3 часов ручной работы до 5 минут автоматизации*

[Проблема](#-проблема) • [Решение](#-решение) • [Результаты](#-результаты) • [Установка](#-установка) • [Использование](#-использование)

</div>

---

## 📋 О проекте

Этот проект — **автоматизированный Python-скрипт**, разработанный по личной инициативе для анализа и контроля складских мощностей на крупном распределительном центре Wildberries.

### 🎯 Контекст

На складе с **адресным хранением** и множеством категорий товаров требовалось ежедневно отслеживать заполненность стеллажей, контролировать соблюдение правил размещения и планировать приемку новых товаров.

---

## 🎯 Проблема

<div align="center">

### Ручной процесс был неэффективным

</div>

<table width="100%">
<tr>
<td width="33%" align="center" valign="top">

### ⏰ Затраты времени

**2-3 часа**  
рабочего времени  
одного сотрудника  
на каждый отчет

</td>
<td width="33%" align="center" valign="top">

### ❌ Ошибки

**Человеческий фактор**  
приводил к неточностям  
в расчетах и  
некорректным решениям

</td>
<td width="33%" align="center" valign="top">

### 📊 Устаревшие данные

**Отчеты устаревали**  
через несколько часов,  
не отражая  
текущую ситуацию

</td>
</tr>
</table>

### Конкретные проблемы:

- 📝 Сотруднику приходилось вручную выгружать данные из системы
- 🧮 Расчеты заполненности по категориям делались в Excel
- 📉 Анализ по этажам, рядам и стеллажам занимал основную часть времени
- 🔄 К моменту завершения отчета данные уже устаревали
- ⚠️ Регулярно возникали ошибки в расчетах из-за человеческого фактора

---

## 💡 Решение

<div align="center">

### Полная автоматизация процесса

</div>

Я разработал **Python-скрипт**, который выполняет все операции автоматически:

### 🔄 Архитектура решения

<table width="100%">
<tr>
<td width="33%" align="center" valign="top">

**1️⃣ Сбор данных**

📡 Подключение к API сервера  
📥 Запрос актуальных данных  
🗄️ Получение информации о товарах и размещении

</td>
<td width="33%" align="center" valign="top">

**2️⃣ Обработка**

🐼 Анализ с помощью Pandas  
📊 Расчет заполненности по:  
• Категориям товаров  
• Рядам и стеллажам  
• Этажам склада

</td>
<td width="33%" align="center" valign="top">

**3️⃣ Отчетность**

📄 Генерация Excel-отчетов  
☁️ Обновление Google Sheets  
📧 Автоматическая рассылка  
⚠️ Алерты о критичных значениях

</td>
</tr>
</table>

---

## 🚀 Результаты

<div align="center">

### Измеримые улучшения

</div>

<table width="100%">
<tr>
<td width="50%" align="center" valign="top">

### ⏱️ Экономия времени

<div align="center">

**До:** 2-3 часа  
**После:** 5 минут

</div>

#### Сокращение на **97%**

Высвободилось время для более важных задач

</td>
<td width="50%" align="center" valign="top">

### ✅ Точность

<div align="center">

**До:** ~95% (ошибки)  
**После:** 100%

</div>

#### **Нулевой** уровень ошибок

Полностью исключен человеческий фактор

</td>
</tr>
<tr>
<td width="50%" align="center" valign="top">

### 📈 Актуальность

<div align="center">

**До:** Устаревшие данные  
**После:** Real-time данные

</div>

#### Данные в **реальном времени**

Принятие решений на основе актуальной информации

</td>
<td width="50%" align="center" valign="top">

### 💰 ROI

<div align="center">

**Экономия:** ~15 часов/неделю  
**Окупаемость:** < 1 недели

</div>

#### Быстрая окупаемость

Мгновенный эффект от внедрения

</td>
</tr>
</table>

### 🎯 Дополнительные преимущества:

- ✅ **Оптимизация размещения** — эффективное планирование новых товаров
- ✅ **Контроль правил** — автоматическое выявление нарушений раскладки
- ✅ **Прогнозирование** — данные для планирования загрузки склада
- ✅ **Масштабируемость** — легко адаптируется под другие склады

---

## 🛠️ Технологии

<div align="center">

### Стек технологий

![Python](https://img.shields.io/badge/-Python_3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/-Pandas-150458?style=flat&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/-NumPy-013243?style=flat&logo=numpy&logoColor=white)
![Google Sheets API](https://img.shields.io/badge/-Google_Sheets_API-34A853?style=flat&logo=google-sheets&logoColor=white)
![OpenPyXL](https://img.shields.io/badge/-OpenPyXL-217346?style=flat&logo=microsoft-excel&logoColor=white)
![Requests](https://img.shields.io/badge/-Requests-2CA5E0?style=flat&logo=python&logoColor=white)

</div>

---

<div align="center">

<table width="90%">
<thead>
<tr>
<th align="center" width="25%">Библиотека</th>
<th align="center" width="15%">Версия</th>
<th align="left" width="60%">Назначение</th>
</tr>
</thead>
<tbody>
<tr>
<td align="center"><code>pandas</code></td>
<td align="center">2.0+</td>
<td>Обработка и анализ данных, расчеты заполненности</td>
</tr>
<tr>
<td align="center"><code>numpy</code></td>
<td align="center">1.24+</td>
<td>Математические операции и массивы</td>
</tr>
<tr>
<td align="center"><code>gspread</code></td>
<td align="center">5.0+</td>
<td>Работа с Google Sheets API</td>
</tr>
<tr>
<td align="center"><code>oauth2client</code></td>
<td align="center">4.1+</td>
<td>Авторизация Google API</td>
</tr>
<tr>
<td align="center"><code>openpyxl</code></td>
<td align="center">3.1+</td>
<td>Создание и редактирование Excel файлов</td>
</tr>
<tr>
<td align="center"><code>requests</code></td>
<td align="center">2.31+</td>
<td>HTTP-запросы к API сервера</td>
</tr>
<tr>
<td align="center"><code>python-dotenv</code></td>
<td align="center">1.0+</td>
<td>Управление переменными окружения</td>
</tr>
</tbody>
</table>

</div>

---

## 📦 Установка

### Требования:

<div align="center">

<table width="70%">
<tr>
<td align="center" width="50%">

**🐍 Python 3.10+**  
Современная версия Python

</td>
<td align="center" width="50%">

**📊 Google Account**  
Для работы с Google Sheets

</td>
</tr>
</table>

</div>

---


