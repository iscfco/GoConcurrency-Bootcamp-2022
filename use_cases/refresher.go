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
	Read(ctx context.Context, cancel context.CancelFunc) []<-chan models.Pokemon
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
	pokemonChannels := r.Read(ctx, cancel)

	refreshedPokemonStream, err := r.fanIn(ctx, cancel, pokemonChannels)
	if err != nil {
		return fmt.Errorf("cannot refresh pokemons due to err: %v", err)
	}

	var pokemons []models.Pokemon
	for refreshedPokemon := range refreshedPokemonStream {
		pokemons = append(pokemons, refreshedPokemon)
	}

	if err := r.Save(ctx, pokemons); err != nil {
		return fmt.Errorf("cannot save in cache pokemons due to err: %v", err)
	}

	return nil
}

func (r Refresher) fanIn(ctx context.Context, cancel context.CancelFunc, in []<-chan models.Pokemon) (chan models.Pokemon, error) {
	pokemons := make(chan models.Pokemon)
	wg := sync.WaitGroup{}

	for _, pokemonChannel := range in {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cannot continue processing channels due to ctx err: %v", ctx.Err())
		}

		wg.Add(1)
		go func(pokemonChannel <-chan models.Pokemon) {
			defer wg.Done()

			for pokemon := range pokemonChannel {
				if ctx.Err() != nil {
					log.Printf("breaking worker due to invalid context: %v\n", ctx.Err())
					return
				}

				wg.Add(1)
				go func(pokemon models.Pokemon) {
					defer wg.Done()

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

					pokemons <- pokemon
				}(pokemon)
			}
		}(pokemonChannel)
	}

	go func() {
		wg.Wait()
		close(pokemons)
	}()

	return pokemons, nil
}
