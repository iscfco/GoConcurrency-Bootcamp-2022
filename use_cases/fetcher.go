package use_cases

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

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

func (f Fetcher) Fetch(ctx context.Context, from, to int) error {
	pokeChannel := f.generatePokeStream(ctx, from, to)

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

func (f Fetcher) generatePokeStream(ctx context.Context, from, to int) chan models.Pokemon {
	ctx, cancelFn := context.WithCancel(ctx)
	ch := make(chan models.Pokemon)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func(ctx context.Context, cancelFn context.CancelFunc) {
		defer wg.Done()

		for id := from; id <= to; id++ {
			if ctx.Err() != nil {
				log.Printf("breaking 'For' statement from: %d in order to avoid performing unnecessary goroutines", id)
				return
			}

			if id > 1000 {
				time.Sleep(time.Duration(100) * time.Millisecond)
			}

			wg.Add(1)
			go func(ctx context.Context, cancelFn context.CancelFunc, id int) {
				defer wg.Done()
				time.Sleep(time.Duration(100+id) * time.Millisecond)

				if ctx.Err() != nil {
					log.Printf("breaking goroutine (#%d) process due to invalid context: %v\n", id, ctx.Err())
					return
				}

				pokemon, err := f.api.FetchPokemon(id)
				if err != nil {
					log.Printf("cannot get id: %d, due to err: %v\n", id, err)
					cancelFn()
				}
				ch <- pokemon

			}(ctx, cancelFn, id)

		}
	}(ctx, cancelFn)

	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}
