package repositories

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"GoConcurrency-Bootcamp-2022/models"
)

type LocalStorage struct{}

const filePath = "resources/pokemons.csv"

func (l LocalStorage) Write(pokemons []models.Pokemon) error {
	file, fErr := os.Create(filePath)
	defer file.Close()
	if fErr != nil {
		return fErr
	}

	w := csv.NewWriter(file)
	records := buildRecords(pokemons)
	if err := w.WriteAll(records); err != nil {
		return err
	}

	return nil
}

func (l LocalStorage) Read(ctx context.Context, cancel context.CancelFunc) []<-chan models.Pokemon {
	in := generateLines(ctx, cancel)

	worker1 := fanOut(ctx, cancel, in)
	worker2 := fanOut(ctx, cancel, in)
	worker3 := fanOut(ctx, cancel, in)

	return []<-chan models.Pokemon{worker1, worker2, worker3}
}

func generateLines(ctx context.Context, cancel context.CancelFunc) <-chan string {
	out := make(chan string)

	go func() {
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("cannot read file due to err: %v\n", err)
			cancel()
			return
		}
		defer file.Close()

		fileScanner := bufio.NewScanner(file)
		fileScanner.Split(bufio.ScanLines)

		if fileScanner.Scan() {
			fileScanner.Text()
		}

		for fileScanner.Scan() {
			select {
			case <-ctx.Done():
				log.Println("finishing read generate line due to context cancelled")
				return
			default:
				out <- fileScanner.Text()
			}
		}

		close(out)
	}()

	return out
}

func fanOut(ctx context.Context, cancel context.CancelFunc, in <-chan string) <-chan models.Pokemon {
	out := make(chan models.Pokemon)

	go func() {
		defer close(out)

		for rawPokemon := range in {

			if ctx.Err() != nil {
				log.Printf("finishing worker due to invalid context: %v\n", ctx.Err())
				return
			}

			pokemonProperties := strings.Split(rawPokemon, ",")

			id, err := strconv.Atoi(pokemonProperties[0])
			if err != nil {
				log.Printf("cannot get pokemon id due to err: %v\n", err)
				cancel()
				return
			}

			height, err := strconv.Atoi(pokemonProperties[2])
			if err != nil {
				log.Printf("cannot get pokemon height due to err: %v\n", err)
				cancel()
				return
			}

			weight, err := strconv.Atoi(pokemonProperties[3])
			if err != nil {
				log.Printf("cannot get pokemon weight due to err: %v\n", err)
				cancel()
				return
			}

			pokemon := models.Pokemon{
				ID:              id,
				Name:            pokemonProperties[1],
				Height:          height,
				Weight:          weight,
				Abilities:       nil,
				FlatAbilityURLs: pokemonProperties[4],
				EffectEntries:   nil,
			}

			out <- pokemon
		}
	}()

	return out
}

func buildRecords(pokemons []models.Pokemon) [][]string {
	headers := []string{"id", "name", "height", "weight", "flat_abilities"}
	records := [][]string{headers}
	for _, p := range pokemons {
		record := fmt.Sprintf("%d,%s,%d,%d,%s",
			p.ID,
			p.Name,
			p.Height,
			p.Weight,
			p.FlatAbilityURLs)
		records = append(records, strings.Split(record, ","))
	}

	return records
}
