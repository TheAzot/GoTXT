package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Stats struct {
	matches map[string]int
	mutex   sync.Mutex
}

// Функция для загрузки запросов из файла
func loadKeywords() ([]string, error) {
	file, err := os.Open("keywords.txt")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var keywords []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		keyword := strings.TrimSpace(scanner.Text())
		if keyword != "" {
			keywords = append(keywords, keyword)
		}
	}
	return keywords, scanner.Err()
}

// Поиск в файле и запись результатов с удалением дубликатов
func processFile(path string, keywords []string, resultSets map[string]map[string]struct{}, stats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Ошибка при открытии %s: %v\n", path, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Увеличиваем буфер для обработки длинных строк
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 3 {
			continue
		}

		url := strings.ToLower(parts[0])
		login := parts[1]
		password := parts[2]
		
		for _, keyword := range keywords {
			if strings.Contains(url, keyword) {
				credentials := fmt.Sprintf("%s:%s", login, password)
				
				stats.mutex.Lock()
				stats.matches[keyword]++
				
				// Добавляем запись в множество для удаления дубликатов
				if _, ok := resultSets[keyword]; !ok {
					resultSets[keyword] = make(map[string]struct{})
				}
				resultSets[keyword][credentials] = struct{}{}
				stats.mutex.Unlock()
				
				break
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Ошибка при чтении %s: %v\n", path, err)
	}
}

// Поиск всех текстовых файлов в директории
func findTxtFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".txt") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func main() {
	startTime := time.Now()
	
	// Загружаем ключевые слова из файла
	keywords, err := loadKeywords()
	if err != nil {
		fmt.Printf("Ошибка при загрузке ключевых слов: %v\n", err)
		fmt.Println("Создайте файл keywords.txt со списком поисковых запросов (по одному на строку)")
		fmt.Println("Нажмите Enter для выхода...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		return
	}

	if len(keywords) == 0 {
		fmt.Println("Список запросов пуст. Добавьте запросы в файл keywords.txt")
		fmt.Println("Нажмите Enter для выхода...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		return
	}

	// Создаем уникальную папку для этого поиска
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	outputDir := fmt.Sprintf("results_%s", timestamp)
	err = os.MkdirAll(outputDir, 0755)
	if err != nil {
		fmt.Printf("Ошибка при создании выходной директории: %v\n", err)
		return
	}

	// Для каждого ключевого слова создаем множество для хранения уникальных результатов
	resultSets := make(map[string]map[string]struct{})
	stats := &Stats{matches: make(map[string]int)}

	// Проверяем существование директории с базами
	if _, err := os.Stat("databases"); os.IsNotExist(err) {
		fmt.Println("Директория 'databases' не найдена. Создайте эту директорию и поместите в неё базы данных.")
		fmt.Println("Нажмите Enter для выхода...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		return
	}

	// Ищем все текстовые файлы в директории с базами
	txtFiles, err := findTxtFiles("databases")
	if err != nil {
		fmt.Printf("Ошибка при поиске текстовых файлов: %v\n", err)
		return
	}

	fmt.Printf("Найдено %d баз данных для обработки\n", len(txtFiles))
	fmt.Printf("Будет выполнен поиск по %d запросам\n", len(keywords))

	var wg sync.WaitGroup

	// Обрабатываем каждый файл в отдельной горутине
	for _, file := range txtFiles {
		wg.Add(1)
		go processFile(file, keywords, resultSets, stats, &wg)
	}

	// Ожидаем завершения всех горутин
	wg.Wait()

	// Записываем результаты в файлы
	totalResults := 0
	for keyword, resultSet := range resultSets {
		outputPath := filepath.Join(outputDir, keyword+".txt")
		f, err := os.Create(outputPath)
		if err != nil {
			fmt.Printf("Ошибка при создании выходного файла для %s: %v\n", keyword, err)
			continue
		}

		writer := bufio.NewWriter(f)
		for credential := range resultSet {
			writer.WriteString(credential + "\n")
			totalResults++
		}

		writer.Flush()
		f.Close()

		fmt.Printf("Запрос '%s': найдено %d совпадений, сохранено %d уникальных записей\n", 
			keyword, stats.matches[keyword], len(resultSet))
	}

	elapsedTime := time.Since(startTime)
	fmt.Printf("\nПоиск завершен за %s\n", elapsedTime)
	fmt.Printf("Всего найдено: %d уникальных записей по %d запросам\n", totalResults, len(keywords))
	fmt.Printf("Результаты сохранены в директорию: %s\n", outputDir)

	fmt.Println("\nНажмите Enter для выхода...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}