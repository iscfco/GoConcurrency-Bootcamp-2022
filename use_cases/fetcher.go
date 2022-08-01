package use_cases

import (
	"log"
	"strings"
	"sync"

	"GoConcurrency-Bootcamp-2022/models"
)

type api interface {
	FetchPokemon(id int) (models.Pokemon, error)
}

type writer interface {
	Write(pokemons []models.Pokemon) error
}

type Fetcher struct {
	api     api
	storage writer
}

func NewFetcher(api api, storage writer) Fetcher {
	return Fetcher{api, storage}
}

func (f Fetcher) Fetch(from, to int) error {
	pokeChannel := f.generatePokeStream(from, to)

	var pokemons []models.Pokemon
	for pokemon := range pokeChannel {
		var flatAbilities []string
		for _, t := range pokemon.Abilities {
			flatAbilities = append(flatAbilities, t.Ability.URL)
		}
		pokemon.FlatAbilityURLs = strings.Join(flatAbilities, "|")

		pokemons = append(pokemons, pokemon)
	}

	return f.storage.Write(pokemons)
}

func (f Fetcher) generatePokeStream(from, to int) chan models.Pokemon {
	ch := make(chan models.Pokemon)
	wg := sync.WaitGroup{}

	for id := from; id <= to; id++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			pokemon, err := f.api.FetchPokemon(id)
			if err != nil {
				log.Printf("cannot get id: %d, due to err: %v", id, err)
			}

			ch <- pokemon
		}(id)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}
