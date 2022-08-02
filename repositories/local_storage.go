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
	"sync"

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

func (l LocalStorage) ReadOld() ([]models.Pokemon, error) {
	file, fErr := os.Open(filePath)
	defer file.Close()
	if fErr != nil {
		return nil, fErr
	}

	r := csv.NewReader(file)
	records, rErr := r.ReadAll()
	if rErr != nil {
		return nil, rErr
	}

	pokemons, err := parseCSVData(records)
	if err != nil {
		return nil, err
	}

	return pokemons, nil
}

func (l LocalStorage) Read(ctx context.Context, cancel context.CancelFunc) chan models.Pokemon {
	in := generateLines(ctx, cancel)

	return fanIn(ctx, cancel, in)
}

func generateLines(ctx context.Context, cancel context.CancelFunc) chan string {
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

func fanIn(ctx context.Context, cancel context.CancelFunc, in chan string) chan models.Pokemon {
	out := make(chan models.Pokemon)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		var i int = -1
		for rawPokemon := range in {
			i++
			if i == 0 {
				continue
			}

			if ctx.Err() != nil {
				log.Printf("breaking 'For' statement from: %d in order to avoid performing unnecessary goroutines", i)
				return
			}

			wg.Add(1)
			go func(rawPokemon string) {
				defer wg.Done()

				if ctx.Err() != nil {
					log.Printf("breaking goroutine (#%d) process due to invalid context: %v\n", i, ctx.Err())
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
			}(rawPokemon)

		}
	}()

	go func() {
		wg.Wait()
		close(out)
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

func parseCSVData(records [][]string) ([]models.Pokemon, error) {
	var pokemons []models.Pokemon
	for i, record := range records {
		if i == 0 {
			continue
		}

		id, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, err
		}

		height, err := strconv.Atoi(record[2])
		if err != nil {
			return nil, err
		}

		weight, err := strconv.Atoi(record[3])
		if err != nil {
			return nil, err
		}

		pokemon := models.Pokemon{
			ID:              id,
			Name:            record[1],
			Height:          height,
			Weight:          weight,
			Abilities:       nil,
			FlatAbilityURLs: record[4],
			EffectEntries:   nil,
		}
		pokemons = append(pokemons, pokemon)
	}

	return pokemons, nil
}
