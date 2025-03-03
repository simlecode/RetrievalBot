package main

import (
	"context"
	"github.com/data-preservation-programs/RetrievalBot/integration/filplus/util"
	"github.com/data-preservation-programs/RetrievalBot/pkg/env"
	"github.com/data-preservation-programs/RetrievalBot/pkg/model"
	"github.com/data-preservation-programs/RetrievalBot/pkg/resolver"
	"github.com/data-preservation-programs/RetrievalBot/pkg/task"
	logging "github.com/ipfs/go-log/v2"
	_ "github.com/joho/godotenv/autoload"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

var logger = logging.Logger("filplus-integration")

func main() {
	filplus := NewFilPlusIntegration()
	for {
		err := filplus.RunOnce(context.TODO())
		if err != nil {
			logger.Error(err)
		}

		time.Sleep(time.Minute)
	}
}

type FilPlusIntegration struct {
	taskCollection        *mongo.Collection
	marketDealsCollection *mongo.Collection
	resultCollection      *mongo.Collection
	batchSize             int
	requester             string
	locationResolver      resolver.LocationResolver
	providerResolver      resolver.ProviderResolver
	ipInfo                resolver.IPInfo
	randConst             float64
}

func NewFilPlusIntegration() *FilPlusIntegration {
	ctx := context.Background()
	taskClient, err := mongo.
		Connect(ctx, options.Client().ApplyURI(env.GetRequiredString(env.QueueMongoURI)))
	if err != nil {
		panic(err)
	}
	taskCollection := taskClient.
		Database(env.GetRequiredString(env.QueueMongoDatabase)).Collection("task_queue")

	stateMarketDealsClient, err := mongo.
		Connect(ctx, options.Client().ApplyURI(env.GetRequiredString(env.StatemarketdealsMongoURI)))
	if err != nil {
		panic(err)
	}
	marketDealsCollection := stateMarketDealsClient.
		Database(env.GetRequiredString(env.StatemarketdealsMongoDatabase)).
		Collection("state_market_deals")

	resultClient, err := mongo.Connect(ctx, options.Client().ApplyURI(env.GetRequiredString(env.ResultMongoURI)))
	if err != nil {
		panic(err)
	}
	resultCollection := resultClient.
		Database(env.GetRequiredString(env.ResultMongoDatabase)).
		Collection("task_result")

	batchSize := env.GetInt(env.FilplusIntegrationBatchSize, 100)
	providerCacheTTL := env.GetDuration(env.ProviderCacheTTL, 24*time.Hour)
	locationCacheTTL := env.GetDuration(env.LocationCacheTTL, 24*time.Hour)
	locationResolver := resolver.NewLocationResolver(env.GetRequiredString(env.IPInfoToken), providerCacheTTL)
	providerResolver, err := resolver.NewProviderResolver(
		env.GetString(env.LotusAPIUrl, "https://api.node.glif.io/rpc/v0"),
		env.GetString(env.LotusAPIToken, ""),
		locationCacheTTL)
	if err != nil {
		panic(err)
	}

	// Check public IP address
	ipInfo, err := resolver.GetPublicIPInfo(ctx, "", "")
	if err != nil {
		panic(err)
	}

	logger.With("ipinfo", ipInfo).Infof("Public IP info retrieved")

	return &FilPlusIntegration{
		taskCollection:        taskCollection,
		marketDealsCollection: marketDealsCollection,
		batchSize:             batchSize,
		requester:             "filplus",
		locationResolver:      locationResolver,
		providerResolver:      *providerResolver,
		resultCollection:      resultCollection,
		ipInfo:                ipInfo,
		randConst:             env.GetFloat64(env.FilplusIntegrationRandConst, 4.0),
	}
}

func (f *FilPlusIntegration) RunOnce(ctx context.Context) error {
	logger.Info("start running filplus integration")

	// If the task queue already have batch size tasks, do nothing
	count, err := f.taskCollection.CountDocuments(ctx, bson.M{"requester": f.requester})
	if err != nil {
		return errors.Wrap(err, "failed to count tasks")
	}

	logger.With("count", count).Info("Current number of tasks in the queue")

	if count > 0 {
		logger.Infof("task queue still have %d tasks, do nothing", count)

		/* Remove old tasks that has stayed in the queue for too long
		_, err = f.taskCollection.DeleteMany(ctx,
			bson.M{"requester": f.requester, "created_at": bson.M{"$lt": time.Now().UTC().Add(-24 * time.Hour)}})
		if err != nil {
			return errors.Wrap(err, "failed to remove old tasks")
		}
		*/
		return nil
	}

	// Get random documents from state_market_deals that are still active and is verified
	aggregateResult, err := f.marketDealsCollection.Aggregate(ctx, bson.A{
		bson.M{"$sample": bson.M{"size": f.batchSize}},
		bson.M{"$match": bson.M{
			"verified":   true,
			"expiration": bson.M{"$gt": time.Now().UTC()},
		}},
	})

	if err != nil {
		return errors.Wrap(err, "failed to get sample documents")
	}

	var documents []model.DealState
	err = aggregateResult.All(ctx, &documents)
	if err != nil {
		return errors.Wrap(err, "failed to decode documents")
	}

	documents = RandomObjects(documents, len(documents)/2, f.randConst)
	tasks, results := util.AddTasks(ctx, f.requester, f.ipInfo, documents, f.locationResolver, f.providerResolver)

	if len(tasks) > 0 {
		_, err = f.taskCollection.InsertMany(ctx, tasks)
		if err != nil {
			return errors.Wrap(err, "failed to insert tasks")
		}
	}

	logger.With("count", len(tasks)).Info("inserted tasks")

	countPerCountry := make(map[string]int)
	countPerContinent := make(map[string]int)
	countPerModule := make(map[task.ModuleName]int)
	for _, t := range tasks {
		//nolint:forcetypeassert
		tsk := t.(task.Task)
		country := tsk.Provider.Country
		continent := tsk.Provider.Continent
		module := tsk.Module
		countPerCountry[country]++
		countPerContinent[continent]++
		countPerModule[module]++
	}

	for country, count := range countPerCountry {
		logger.With("country", country, "count", count).Info("tasks per country")
	}

	for continent, count := range countPerContinent {
		logger.With("continent", continent, "count", count).Info("tasks per continent")
	}

	for module, count := range countPerModule {
		logger.With("module", module, "count", count).Info("tasks per module")
	}

	if len(results) > 0 {
		_, err = f.resultCollection.InsertMany(ctx, results)
		if err != nil {
			return errors.Wrap(err, "failed to insert results")
		}
	}

	logger.With("count", len(results)).Info("inserted results")

	return nil
}
