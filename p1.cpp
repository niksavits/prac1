#include <iostream>
#include <algorithm>
#include <string>
#include <sstream>
#include <fstream>
#include <pthread.h>
#include <sys/stat.h>
#include <dirent.h>
#include <cstring>
#include <unistd.h>

using namespace std;

#define MAX_TABLES 100
#define MAX_COLUMNS 256
#define MAX_ROWS 1000

struct SimpleJSON {
    string name;
    int tuples_limit;
    struct {  // Вложенная структура для таблицы и колонок
        string table_name;
        string columns[MAX_COLUMNS];
        int columns_count;
    } structure[MAX_TABLES];
    int structure_size = 0;

    SimpleJSON(const string& config_file) {
        ifstream file(config_file);
        if (!file) {
            cerr << "Не удалось открыть файл конфигурации: " << config_file << endl;  // Вывод ошибки
            return;
        }

        string line;
        while (getline(file, line)) {
            if (line.find("\"name\"") != string::npos) {  // Поиск строки с именем схемы
                name = clean_string(extract_value(line));  // Извлекаем и очищаем строку с именем
            } else if (line.find("\"tuples_limit\"") != string::npos) {  // Поиск лимита кортежей
                tuples_limit = stoi(extract_value(line));  // Преобразуем строку в число и сохраняем лимит
            } else if (line.find("\"table_name\"") != string::npos) {  // Поиск имени таблицы
                structure[structure_size].table_name = clean_string(extract_value(line));  // Извлекаем и сохраняем имя таблицы
            } else if (line.find("\"columns\"") != string::npos) {  // Поиск колонок таблицы
                structure[structure_size].columns_count = extract_columns(file, structure[structure_size].columns);  // Извлекаем колонки
                structure_size++;
            }
        }
    }

    string extract_value(const string& line) {
        size_t pos = line.find(":");  // Ищем двоеточие, разделяющее ключ и значение
        if (pos != string::npos) {  // Если двоеточие найдено
            string value = line.substr(pos + 1);  // Извлекаем значение после двоеточия
            value.erase(0, value.find_first_not_of(" \"\n\r"));  // Удаляем пробелы, кавычки и символы новой строки в начале
            value.erase(value.find_last_not_of(" \"\n\r,") + 1);  // Удаляем пробелы, кавычки, запятые в конце
            return value;  // Возвращаем очищенное значение
        }
        return "";  // Если двоеточие не найдено, возвращаем пустую строку
    }

    string clean_string(const string& line) {
        string value = line;
        if (!value.empty() && value[0] == '"') {  // Если строка начинается с кавычки
            value.erase(0, 1);  // Удаляем первую кавычку
        }
        if (!value.empty() && (value.back() == '"' || value.back() == ',')) {  // Если строка заканчивается кавычкой или запятой
            value.pop_back();  // Удаляем последний символ
        }
        value.erase(0, value.find_first_not_of(" "));
        value.erase(value.find_last_not_of(" ") + 1);
        return value;
    }

    int extract_columns(ifstream& file, string columns[]) {
        int count = 0;
        string line;

        if (!file.is_open()) {
            cerr << "Ошибка: Не удалось открыть файл!" << endl;
            return count;
        }

        while (getline(file, line)) {
            line.erase(remove(line.begin(), line.end(), '\"'), line.end());
            line.erase(remove(line.begin(), line.end(), ','), line.end());
            line.erase(remove(line.begin(), line.end(), ' '), line.end());

            if (line == "]") {
                break;
            }

            if (!line.empty()) {  // Проверяем, что строка не пустая
                columns[count++] = line;  // Сохраняем колонку в массив
            }
        }

        return count;
    }
};

struct TableLock {
    pthread_mutex_t lock;  // Мьютекс для синхронизации доступа к таблицам

    TableLock() {
        pthread_mutex_init(&lock, nullptr);
    }

    ~TableLock() {
        pthread_mutex_destroy(&lock);
    }

    void lock_table(const string& table_path) {
        pthread_mutex_lock(&lock);  // Блокируем мьютекс
        ofstream lock_file(table_path + "_lock");  // Открываем файл блокировки
        lock_file << "locked";  // Записываем состояние блокировки
        lock_file.close();  // Закрываем файл
    }

    void unlock_table(const string& table_path) {
        ofstream lock_file(table_path + "_lock");
        lock_file << "unlocked";
        lock_file.close();
        pthread_mutex_unlock(&lock);
    }
};

struct Table {
    string table_name;
    string columns[MAX_COLUMNS];
    int columns_count;
    int tuples_limit;
    string table_path;
    TableLock table_lock;  // Объект для блокировки таблицы
    int pk_sequence;  // Счетчик для первичного ключа

    Table() : tuples_limit(0), pk_sequence(1) {}

    Table(const string& name, string cols[], int columns_count, int limit, const string& schema_name)
        : columns_count(columns_count), tuples_limit(limit), pk_sequence(1) {
        table_name = name;
        table_path = schema_name + "/" + table_name; // Формируем путь к файлу таблицы

    mkdir(table_path.c_str(), 0777);

    for (int i = 0; i < columns_count; ++i) {
        columns[i] = cols[i];
    }

    ofstream pk_file(table_path + "/" + table_name + "_pk_sequence");
    pk_file << pk_sequence;
    pk_file.close();
}

string get_next_file() {
    int file_number = 1;
    string filename;
    while (true) {
        filename = table_path + "/" + to_string(file_number) + ".csv";
        ifstream file(filename);
        if (!file) {
            break;
        }
        if (get_row_count(filename) < tuples_limit) {
            return filename;
        }
        file_number++;
    }
    return table_path + "/" + to_string(file_number) + ".csv";
}

int get_row_count(const string& file) {
    ifstream infile(file);
    string line;
    int count = 0;
    while (getline(infile, line)) {
        count++;
    }
    return count - 1;
}

void increment_pk() {
    pk_sequence++;
    // Записываем новое значение в файл
    ofstream pk_file(table_path + "/" + table_name + "_pk_sequence");
    pk_file << pk_sequence;
    pk_file.close();
}

void insert_row(string values[], int values_count) {
    table_lock.lock_table(table_path);  // Блокируем таблицу

    string current_file = get_next_file();  // Получаем имя файла для вставки

    ifstream check_file(current_file);
    bool is_empty = check_file.peek() == ifstream::traits_type::eof();  // Проверяем конец файла
    check_file.close();

    if (is_empty) {  // Если файл пустой, записываем заголовок
        ofstream csv_out(current_file);
        csv_out << table_name + "_pk,";  // Записываем первичный ключ
        for (int i = 0; i < columns_count; ++i) {
            csv_out << columns[i];  // Записываем имена колонок
            if (i != columns_count - 1) {
                csv_out << ",";  // Добавляем запятую, если это не последняя колонка
            }
        }
        csv_out << "\n";
        csv_out.close();
    }

    // Записываем новую строку данных в файл
    ofstream file_out(current_file, ios::app);  // Открываем файл для добавления
    file_out << pk_sequence << ",";  // Записываем первичный ключ
    for (int i = 0; i < values_count; ++i) {
        file_out << values[i];  // Записываем значения колонок
        if (i != values_count - 1) {
            file_out << ",";  // Добавляем запятую
        }
    }
    file_out << "\n";  // Переходим на новую строку
    file_out.close();

    increment_pk();  // Увеличиваем значение первичного ключа
    table_lock.unlock_table(table_path);  // Разблокируем таблицу
}

void delete_rows(const string& condition) {
    table_lock.lock_table(table_path);  // Блокируем таблицу

    string temp_file_path = table_path + "/temp.csv";  // Временный файл для записи данных
    string file_path = get_next_file();  // Получаем имя файла с таблицей
    ifstream infile(file_path);  // Открываем файл для чтения
    ofstream temp_file(temp_file_path);  // Открываем временный файл для записи
    string line;  // Переменная для хранения строк

    cout << "Удаление из таблицы: " << table_name << " с условием: '" << condition << "'" << endl;

    getline(infile, line);  // Читаем заголовок таблицы
    temp_file << line << "\n";  // Записываем заголовок во временный файл

    while (getline(infile, line)) {
        cout << "Обрабатываем строку: " << line << endl;

        if (evaluate_where_clause(line, condition)) {
            cout << "Строка соответствует условию: " << line << endl;
        } else {
            cout << "Строка не соответствует условию: " << line << endl;
            temp_file << line << "\n";
        }
    }

    infile.close();
    temp_file.close();

    remove(file_path.c_str());
    rename(temp_file_path.c_str(), file_path.c_str());

    table_lock.unlock_table(table_path);  // Разблокируем таблицу
}

void select_rows(const string columns[], int col_count, const string& where_clause) {
    string file_path = get_next_file();
    ifstream infile(file_path);

    if (!infile) {
        cerr << "Ошибка: Не удалось открыть файл " << file_path << endl;
        return;
    }

    string line;
    bool is_first_line = true;  // Флаг для пропуска заголовка
    bool has_output = false;  // Флаг, указывающий, были ли результаты

    while (getline(infile, line)) {
        if (is_first_line) {
            is_first_line = false;  // Пропускаем заголовок
            continue;
        }

        // Проверяем, удовлетворяет ли строка условию WHERE
        if (evaluate_where_clause(line, where_clause)) {
            if (!has_output) {
                cout << "Вывод выбранных колонок:" << endl;
                has_output = true;
            }
            print_selected_columns(line, columns, col_count);
        }
    }

    if (!has_output) {
        cout << "Нет данных, соответствующих условиям." << endl;
    }

    infile.close();
}

int get_column_index(const string& column_name) {
    string table_prefix;
    string actual_column;

    // Если колонка имеет формат "table.column", разбиваем её
    size_t dot_pos = column_name.find('.');
    if (dot_pos != string::npos) {
        table_prefix = column_name.substr(0, dot_pos);
        actual_column = column_name.substr(dot_pos + 1);
    } else {
        actual_column = column_name;
    }

    // Проверка, является ли колонка частью данной таблицы
    for (int i = 0; i < columns_count; ++i) {
        if (columns[i] == actual_column) {
            // Если префикс таблицы указан, проверяем его соответствие
            if (!table_prefix.empty() && table_prefix != table_name) {
                continue;  // Пропускаем, если префикс не совпадает с текущей таблицей
            }
            return i + 1;  // Возвращаем индекс колонки
        }
    }
    return -1;  // Если колонка не найдена
}


// Функция для проверки условия WHERE для строки
bool evaluate_where_clause(const string& row, const string& where_clause) {
    cout << "Evaluating WHERE clause: " << where_clause << endl;

    if (where_clause.empty()) {
        cout << "WHERE clause is empty. Returning true." << endl;
        return true;
    }

    size_t pos_eq = where_clause.find('=');
    size_t pos_gt = where_clause.find('>');
    size_t pos_lt = where_clause.find('<');

    // Определение позиции первого найденного оператора
    size_t pos = string::npos;
    if (pos_eq != string::npos) pos = pos_eq;
    if (pos_gt != string::npos && (pos == string::npos || pos_gt < pos)) pos = pos_gt;
    if (pos_lt != string::npos && (pos == string::npos || pos_lt < pos)) pos = pos_lt;

    if (pos == string::npos) {
        cout << "Invalid WHERE clause format. No comparison operator found." << endl;
        return false;
    }

    string column_name = where_clause.substr(0, pos);
    string value = where_clause.substr(pos + 1);

    column_name.erase(remove(column_name.begin(), column_name.end(), ' '), column_name.end());
    value.erase(remove(value.begin(), value.end(), ' '), value.end());
    value.erase(remove(value.begin(), value.end(), '\''), value.end());  // Удаляем кавычки

    int col_index = get_column_index(column_name);
    cout << "Index for column '" << column_name << "': " << col_index << endl;

    if (col_index == -1) {
        cout << "Column not found: " << column_name << ". Returning false." << endl;
        return false;
    }

    // Разбиваем строку на отдельные ячейки
    stringstream row_ss(row);
    string cell;
    int current_index = 0;

    // Проходим по всем ячейкам строки
    while (getline(row_ss, cell, ',')) {
        // Удаляем пробелы и кавычки
        cell.erase(remove(cell.begin(), cell.end(), ' '), cell.end());
        cell.erase(remove(cell.begin(), cell.end(), '\"'), cell.end());

        cout << "Current cell [" << current_index << "]: '" << cell << "'" << endl;

        // Если индекс текущей ячейки соответствует индексу колонки
        if (current_index == col_index) {
            // Сравнение значения в ячейке с ожидаемым значением
            if (pos_eq != string::npos) {
                return cell == value;  // Сравнение на равенство
            } else if (pos_gt != string::npos) {
                return stoi(cell) > stoi(value);  // Сравнение на больше
            } else if (pos_lt != string::npos) {
                return stoi(cell) < stoi(value);  // Сравнение на меньше
            }
        }
        current_index++;
    }

    cout << "Returning false based on matches." << endl;
    return false;  // Условия не выполнены, возвращаем false
}

// Функция для печати выбранных колонок из строки
void print_selected_columns(const string& row, const string columns[], int col_count) {
    stringstream ss(row);  // Создаем строковый поток для строки данных
    string cell;  // Переменная для хранения значения ячейки
    int col_idx = 0;  // Индекс текущей колонки
    bool is_first_row = true;  // Флаг для пропуска первичного ключа

    while (getline(ss, cell, ',')) {
        if (is_first_row) {
            is_first_row = false;
            col_idx++;
            continue;
        }

        if (col_idx == 0) {
            col_idx++;
            continue;
        }

        // Выводим только те колонки, которые были указаны в запросе
        if (col_idx - 1 < col_count) {
            cout << columns[col_idx - 1] << " - " << cell << endl;
        }
        col_idx++;
    }
}
};

struct Database {
    string schema_name;
    int tuples_limit;
    Table tables[MAX_TABLES];
    int tables_count = 0;

    Database(const string& config_file) {
        SimpleJSON schema(config_file);

        schema_name = schema.name;
        tuples_limit = schema.tuples_limit;

        mkdir(schema_name.c_str(), 0777);

        for (int i = 0; i < schema.structure_size; ++i) {
            // Создаем новую таблицу на основе данных из схемы и добавляем её в массив таблиц
            Table new_table(schema.structure[i].table_name, schema.structure[i].columns, schema.structure[i].columns_count, tuples_limit, schema_name);
            tables[tables_count++] = new_table;
        }
    }

    Table* find_table(const string& table_name) {
    for (int i = 0; i < tables_count; ++i) {
        cout << "Проверяем таблицу: " << tables[i].table_name << endl;  // Вывод отладочной информации
        if (tables[i].table_name == table_name) {
            return &tables[i];
        }
    }
    cout << "Таблица не найдена: " << table_name << endl;
    return nullptr;
}

    void insert_into(const string& table_name, string values[], int values_count) {
        Table* table = find_table(table_name);
        if (table) {
            table->insert_row(values, values_count);
        } else {
            cerr << "Таблица не найдена: " << table_name << endl;
        }
    }

    void delete_from(const string& table_name, const string& condition) {
        Table* table = find_table(table_name);
        if (table) {
            table->delete_rows(condition);
        } else {
            cerr << "Таблица не найдена!" << endl;
        }
    }

    void select_from(const string& table_name, const string columns[], int col_count, const string& where_clause) {
        Table* table = find_table(table_name);
        if (table) {
            table->select_rows(columns, col_count, where_clause);
        } else {
            cerr << "Таблица не найдена!" << endl;
        }
    }

    void select_from_multiple(const string& table_name1, const string& table_name2, const string columns[], int col_count, const string& where_clause) {
        Table* table1 = find_table(table_name1);
        Table* table2 = find_table(table_name2);

        if (!table1) {
            cerr << "Таблица '" << table_name1 << "' не найдена!" << endl;
            return;
        }

        if (!table2) {
            cout << "Таблица '" << table_name2 << "' не найдена. Выполняем выборку только из '" << table_name1 << "'." << endl;
            table1->select_rows(columns, col_count, where_clause);
            return;
        }

        string file_path1 = table1->get_next_file();
        string file_path2 = table2->get_next_file();

        ifstream infile1(file_path1);
        ifstream infile2(file_path2);

        string rows1[MAX_ROWS];
        string rows2[MAX_ROWS];
        int count1 = 0;
        int count2 = 0;

        while (getline(infile1, rows1[count1]) && count1 < MAX_ROWS) {
            count1++;
        }

        while (getline(infile2, rows2[count2]) && count2 < MAX_ROWS) {
            count2++;
        }

        bool has_output = false;  // Флаг, указывающий на наличие выводимых данных

        for (int i = 1; i < count1; i++) {
            for (int j = 1; j < count2; j++) {
                string combined_row = rows1[i] + "," + rows2[j];  // Объединяем строки из обеих таблиц

                if (table1->evaluate_where_clause(combined_row, where_clause)) {
                    if (!has_output) {
                        cout << "Вывод выбранных колонок из объединенных таблиц:" << endl;
                        has_output = true;
                    }
                    table1->print_selected_columns(combined_row, columns, col_count);
                }
            }
        }

        if (!has_output) {
            cout << "Нет данных, соответствующих условиям." << endl;
        }

        infile1.close();
        infile2.close();
    }
};
struct DynamicArray {
    string* data;
    int size;
    int capacity;

    // Конструктор, инициализация массива
    DynamicArray() {
        size = 0;
        capacity = 10; // Начальная ёмкость массива
        data = new string[capacity];
    }

    // Функция добавления элемента в массив
    void push_back(const string& value) {
        if (size == capacity) {
            // Расширяем массив, если достигнут предел
            capacity *= 2;
            string* new_data = new string[capacity];
            for (int i = 0; i < size; i++) {
                new_data[i] = data[i];
            }
            delete[] data;
            data = new_data;
        }
        data[size] = value;
        size++;
    }

    // Доступ к элементам массива
    string& operator[](int index) {
        return data[index];
    }

    // Освобождение памяти
    ~DynamicArray() {
        delete[] data;
    }
};

struct SQLParser {
    static void execute_query(const string& query, Database& db) {
        istringstream iss(query);  // Создаем поток для обработки SQL-запроса
        string command;
        iss >> command;

        if (command == "INSERT") {
            handle_insert(iss, db);
        } else if (command == "SELECT") {
            handle_select(iss, db);
        } else if (command == "DELETE") {
            handle_delete(iss, db);
        } else {
            cerr << "Неизвестная SQL-команда: " << command << endl;
        }
    }

private:
    static void handle_insert(istringstream& iss, Database& db) {
        string into, table_name, values;
        iss >> into >> table_name;

        string row_data;
        getline(iss, row_data);

        size_t start = row_data.find("(");
        size_t end = row_data.find(")");
        if (start != string::npos && end != string::npos && end > start) {
            row_data = row_data.substr(start + 1, end - start - 1);  // Извлекаем строку с данными
        } else {
            cerr << "Ошибка в синтаксисе INSERT-запроса." << endl;
            return;
        }

        string row_values[MAX_COLUMNS];
        int values_count = 0;

        stringstream ss(row_data);
        string value;
        while (getline(ss, value, ',')) {

            value.erase(remove(value.begin(), value.end(), '\"'), value.end());
            value.erase(remove(value.begin(), value.end(), ' '), value.end());
            row_values[values_count++] = value;
        }

        db.insert_into(table_name, row_values, values_count);
        cout << "Команда INSERT выполнена успешно" << endl;
    }

    static void handle_delete(istringstream& iss, Database& db) {
        string from, table_name, condition;
        iss >> from >> table_name;
        getline(iss, condition);

        if (!condition.empty() && condition.find("WHERE") != string::npos) {
            condition = condition.substr(condition.find("WHERE") + 6);
        } else {
            condition.clear();
        }

        db.delete_from(table_name, condition);  // Удаляем строки, соответствующие условию
        cout << "Команда DELETE выполнена успешно" << endl;
    }

    static void handle_select(istringstream& iss, Database& db) {
    string select_part, from_part, where_clause;
    string query = iss.str();

    // Ищем ключевые слова в строке запроса
    size_t from_pos = query.find("FROM");
    size_t where_pos = query.find("WHERE");

    // Если нет ключевого слова FROM, это некорректный запрос
    if (from_pos == string::npos) {
        cerr << "Ошибка: Ключевое слово FROM не найдено в запросе." << endl;
        return;
    }

    // Извлекаем части запроса
    select_part = query.substr(0, from_pos); // До FROM - часть SELECT
    if (where_pos != string::npos) {
        from_part = query.substr(from_pos + 5, where_pos - from_pos - 5); // Между FROM и WHERE
        where_clause = query.substr(where_pos + 6); // После WHERE
    } else {
        from_part = query.substr(from_pos + 5); // После FROM до конца
        where_clause = ""; // Если нет WHERE, условие пустое
    }

    // Убираем лишние пробелы в каждой части
    trim(select_part);
    trim(from_part);
    trim(where_clause);

    // Разбираем колонки из части SELECT
    string columns_str;
    istringstream select_iss(select_part);
    select_iss >> columns_str; // Получаем ключевое слово SELECT
    if (columns_str != "SELECT") {
        cerr << "Ошибка: Ожидалось ключевое слово SELECT." << endl;
        return;
    }

    string column_names = select_part.substr(select_part.find("SELECT") + 7); // Колонки после SELECT

    // Используем динамический массив для хранения колонок
    DynamicArray parsed_columns;
    parse_columns(column_names, parsed_columns);

    // Разбираем таблицы из секции FROM
    istringstream from_iss(from_part);
    string table_name;

    // Используем динамический массив для хранения таблиц
    DynamicArray table_names;

    // Извлекаем таблицы из секции FROM
    while (getline(from_iss, table_name, ',')) {
        trim(table_name); // Убираем лишние пробелы вокруг названий таблиц
        table_names.push_back(table_name);
    }

    // Проверка, найдены ли все таблицы в базе данных
    DynamicArray tables;
    for (int i = 0; i < table_names.size; ++i) {
        Table* table = db.find_table(table_names[i]);
        if (table) {
            tables.push_back(table->table_name);
        } else {
            cerr << "Таблица не найдена: " << table_names[i] << endl;
            return;
        }
    }

    // Выполняем выборку с учетом WHERE (если оно есть)
    if (table_names.size == 1) {
        db.select_from(tables[0], parsed_columns.data, parsed_columns.size, where_clause);
    } else if (table_names.size == 2) {
        db.select_from_multiple(tables[0], tables[1], parsed_columns.data, parsed_columns.size, where_clause);
    } else {
        cerr << "Ошибка: Запрос может поддерживать только одну или две таблицы." << endl;
    }
}

static void parse_columns(const string& columns_str, DynamicArray& parsed_columns) {
    stringstream ss(columns_str);
    string column;
    while (getline(ss, column, ',')) {
        // Убираем пробелы вокруг колонок
        trim(column);

        // Добавляем колонку в динамический массив
        parsed_columns.push_back(column);
    }
}

static void trim(string& str) {
    size_t first = str.find_first_not_of(' ');
    size_t last = str.find_last_not_of(' ');
    if (first != string::npos && last != string::npos) {
        str = str.substr(first, (last - first + 1));
    } else {
        str.clear();
    }
}
};

int main() {

    Database db("/home/skywalker/Рабочий стол/output/scheme.json");

    string user_query;
    while (true) {
        cout << "Введите SQL-запрос (или 'exit' для выхода): ";
        getline(cin, user_query);

        if (user_query == "exit") {
            break;
        }

        SQLParser::execute_query(user_query, db);
    }

    return 0;
}
