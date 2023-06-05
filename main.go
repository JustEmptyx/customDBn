package main

import (
	"bytes"
	"fmt"
	_ "io/ioutil"
	"math/rand"
	"strings"
	"sync"
	_ "sync"
	//"fmt"
	"strconv"
	"time"
)

func generateId(name string) string {
	curTime := time.Now().UnixNano()
	return strconv.Itoa(int(curTime)) + ":" + name
}

func addManyCollections(Database *Database) {
	nstart := 0
	nend := 1
	for i := nstart; i < nend; i++ {
		tx := Database.WriteTx()
		name := []byte("newCollection" + strconv.Itoa(i))
		tx.CreateCollection(name)
		_ = tx.Commit()
	}
}

// test function to fill collections, full docs in lastT.Database
func addNewElementsToCollection(Database *Database, name string) {
	nstart := 0
	nend := 3
	for i := nstart; i < nend; i++ {
		time.Sleep(1 * time.Second)
		randomId := []byte(generateId(string(rune(rand.Intn(3) + 97))))
		key, value := randomId, randomId
		tx := Database.WriteTx()
		collection, _ := tx.GetCollection([]byte(name))
		_ = collection.Put(key, value)
		_ = tx.Commit()
	}
}

func addNewElementToCollection(Database *Database, name string) {
	tx := Database.WriteTx()
	keyBuf := memset([]byte(name), testValSize)
	collection := tx.getRootCollection()
	collection, _ = tx.GetCollection([]byte("test1"))
	collection.Put(keyBuf, keyBuf)
	tx.Commit()
}

func getSeqOfNodes(Database *Database, seq []int) {
	tx := Database.ReadTx()
	collection := tx.getRootCollection()
	id := collection.ID()
	println(id)
	res, _ := collection.getNodes(seq)
	fmt.Printf("nodes : %s", res)
	_ = tx.Commit()
}
func createItemsCustom(keys []string) []*Item {
	items := make([]*Item, 0)
	for _, key := range keys {
		items = append(items, newItem([]byte(key), []byte(key)))
	}
	return items
}

func addAndRebalanceSplit(Database *Database) {
	tx := Database.WriteTx()
	child0 := tx.createNode(tx.newNode(createItemsCustom([]string{"0:a", "1:a", "2:b", "3:c"}), []pgnum{}))
	child1 := tx.createNode(tx.newNode(createItemsCustom([]string{"5:d", "6:a", "7:a", "8:b"}), []pgnum{}))
	root := tx.createNode(tx.newNode(createItemsCustom([]string{"4:b"}), []pgnum{child0.pageNum, child1.pageNum}))
	tx.createCollection(newCollection(testCollectionName, root.pageNum))
	//val := createItem("9")
	//_ = collection.Put(val, val)
	tx.Commit()
}

func findValueByKey(Database *Database, key string) {
	tx := Database.ReadTx()
	keyBuf := memset([]byte(key), testValSize)
	collection := tx.getRootCollection()
	collection, _ = tx.GetCollection([]byte("test1"))
	item, _ := collection.Find([]byte(keyBuf))
	tx.Commit()
	fmt.Printf("key : %s, value: %s\n", item.key, item.value)
}

func worker(id int, wg *sync.WaitGroup, path string, info string) {
	defer wg.Done()

	fmt.Printf("Worker %d started\n", id)
	Database, _ := Open(path, DefaultParams)
	getAllElementsFromCollectionByDocName(Database, info)
	fmt.Printf("Worker %d finished\n", id)
}

//Пусть мы провели шардирование
//Иммитируем наличие шардов разными файлами базы данных, в которых содержатся различные коллекции
//func imitiateShardedIndexes(Database *Database) {
//	paths :=[4]string{"1.db","2.db","3.db","4.db"}
//	info :=[4]string{"a","b","c","d"}
//	var wg sync.WaitGroup
//	for i := 1; i <= 4; i++ {
//		wg.Add(1)
//		go worker(i, &wg,paths[i],info[i])
//	}
//
//	wg.Wait()
//	fmt.Println("All workers finished")
//}

// !!!Результат 22.05 Немного накосячил, производительность выросла в 3.2 раза примерно.
// !!!Смортеть БД с доками. Скрипт для разбиения отработал, но кривовато, дофиксить
// Дата пока хранится в имени. Перефиксить обратно на индексы
// Слайсинг-, удалить методы слайсинга
func generateRandomString() string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := 500
	result := make([]byte, length)

	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	fmt.Println(string(result))
	return string(result)
}

func getAllElementsHelper(node *Node, allElements []Item, key string) (*Node, []Item, string) {
	if len(node.childNodes) != 0 {
		prevNode := node
		for i := 0; i < len(node.childNodes); i++ {
			node, _ := node.getNode(node.childNodes[i])
			node, allElements, key = getAllElementsHelper(node, allElements, key)
			node = prevNode
		}
	} else {
		for i := range node.items {
			if bytes.Compare([]byte(strings.Split(string(node.items[i].key), ":")[1]), []byte(key)) == 0 {
				allElements = append(allElements, *node.items[i])
			}
		}
		return node, allElements, key
	}
	return node, allElements, key
}

func getAllElementsFromCollectionByDocName(Database *Database, key string) {
	tx := Database.ReadTx()
	c, _ := tx.GetCollection([]byte("test1"))
	n, _ := c.tx.getNode(c.rootNodePage)
	var allElements []Item
	_, allElements, _ = getAllElementsHelper(n, allElements, key)
	fmt.Printf("elements : %s \n", allElements)
	fmt.Println("length : ", len(allElements))
	tx.Commit()
}

func getFilteredElementsFromCollectionByDocName() {

}

func addCollection(Database *Database) {
	tx := Database.WriteTx()
	name := []byte("test1")
	tx.CreateCollection(name)
	_ = tx.Commit()
}

func main() {
	start := time.Now()
	path := "temporal.Database"
	Database, _ := Open(path, DefaultParams)
	//items := createItemsCustom([]string{"1:a", "2:b", "3:c", "4:d"})
	//fmt.Printf("%s", items)
	addCollection(Database)
	//addManyCollections(Database)
	addNewElementsToCollection(Database, "test1")
	//getSeqOfNodes(Database, []int{0, 0})
	//addAndRebalanceSplit(Database)
	//findValueByKey(Database, "0")
	//getAllElementsFromCollectionByDocName(Database, "a")
	//addNewElementToCollection(Database, "d")
	//addNewElementToCollection(Database, "e")

	//imitiateShardedIndexes(Database)
	_ = Database.Close()
	duration := time.Since(start)
	fmt.Println(duration)
}
