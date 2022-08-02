package use_cases

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"GoConcurrency-Bootcamp-2022/models"
)

type reader interface {
	Read(ctx context.Context, cancel context.CancelFunc) chan models.Pokemon
}

type saver interface {
	Save(context.Context, []models.Pokemon) error
}

type fetcher interface {
	FetchAbility(string) (models.Ability, error)
}

type Refresher struct {
	reader
	saver
	fetcher
}

func NewRefresher(reader reader, saver saver, fetcher fetcher) Refresher {
	return Refresher{reader, saver, fetcher}
}

func (r Refresher) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	pokemonStream := r.Read(ctx, cancel)

	pokemons, err := r.fanOut(ctx, cancel, pokemonStream)
	if err != nil {
		return fmt.Errorf("cannot refresh pokemons due to err: %v", err)
	}

	if err := r.Save(ctx, pokemons); err != nil {
		return fmt.Errorf("cannot save in cache pokemons due to err: %v", err)
	}

	return nil
}

func (r Refresher) fanOut(ctx context.Context, cancel context.CancelFunc, in chan models.Pokemon) ([]models.Pokemon, error) {
	var pokemons []models.Pokemon
	wg := sync.WaitGroup{}

	for pokemon := range in {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cannot continue reading pokemon stream from id: %d, due to err: %v", pokemon.ID, ctx.Err())
		}

		wg.Add(1)
		go func(pokemon models.Pokemon) {
			defer wg.Done()

			if ctx.Err() != nil {
				log.Printf("breaking goroutine (#%d) process due to invalid context: %v\n", pokemon.ID, ctx.Err())
				return
			}

			urls := strings.Split(pokemon.FlatAbilityURLs, "|")
			var abilities []string
			for _, url := range urls {
				ability, err := r.FetchAbility(url)
				if err != nil {
					log.Printf("cannot fetch ability for: %d, due to err: %v\n", pokemon.ID, err)
					cancel()
					return
				}

				for _, ee := range ability.EffectEntries {
					abilities = append(abilities, ee.Effect)
				}
			}

			pokemon.EffectEntries = abilities
			pokemons = append(pokemons, pokemon)
		}(pokemon)
	}

	wg.Wait()

	return pokemons, nil
}
