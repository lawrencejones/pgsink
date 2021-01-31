import { ResponseContext, RequestContext, HttpFile } from '../http/http';
import * as models from '../models/all';
import { Configuration} from '../configuration'
import { Observable, of, from } from '../rxjsStub';
import {mergeMap, map} from  '../rxjsStub';

import { CheckResponseBody } from '../models/CheckResponseBody';
import { Import } from '../models/Import';
import { Table } from '../models/Table';

import { HealthApiRequestFactory, HealthApiResponseProcessor} from "../apis/HealthApi";
export class ObservableHealthApi {
    private requestFactory: HealthApiRequestFactory;
    private responseProcessor: HealthApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: HealthApiRequestFactory,
        responseProcessor?: HealthApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new HealthApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new HealthApiResponseProcessor();
    }

    /**
     * Health check for probes
     * Check Health
     */
    public healthCheck(options?: Configuration): Observable<CheckResponseBody> {
    	const requestContextPromise = this.requestFactory.healthCheck(options);

		// build promise chain
    let middlewarePreObservable = from<RequestContext>(requestContextPromise);
    	for (let middleware of this.configuration.middleware) {
    		middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
    	}

    	return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
	    	pipe(mergeMap((response: ResponseContext) => {
	    		let middlewarePostObservable = of(response);
	    		for (let middleware of this.configuration.middleware) {
	    			middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
	    		}
	    		return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.healthCheck(rsp)));
	    	}));
    }
	

}




import { ImportsApiRequestFactory, ImportsApiResponseProcessor} from "../apis/ImportsApi";
export class ObservableImportsApi {
    private requestFactory: ImportsApiRequestFactory;
    private responseProcessor: ImportsApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: ImportsApiRequestFactory,
        responseProcessor?: ImportsApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new ImportsApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new ImportsApiResponseProcessor();
    }

    /**
     * List all imports
     * List Imports
     */
    public importsList(options?: Configuration): Observable<Array<Import>> {
    	const requestContextPromise = this.requestFactory.importsList(options);

		// build promise chain
    let middlewarePreObservable = from<RequestContext>(requestContextPromise);
    	for (let middleware of this.configuration.middleware) {
    		middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
    	}

    	return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
	    	pipe(mergeMap((response: ResponseContext) => {
	    		let middlewarePostObservable = of(response);
	    		for (let middleware of this.configuration.middleware) {
	    			middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
	    		}
	    		return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.importsList(rsp)));
	    	}));
    }
	

}




import { TablesApiRequestFactory, TablesApiResponseProcessor} from "../apis/TablesApi";
export class ObservableTablesApi {
    private requestFactory: TablesApiRequestFactory;
    private responseProcessor: TablesApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: TablesApiRequestFactory,
        responseProcessor?: TablesApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new TablesApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new TablesApiResponseProcessor();
    }

    /**
     * List all tables
     * List Tables
     * @param schema Comma separated list of Postgres schemas to filter by
     */
    public tablesList(schema?: string, options?: Configuration): Observable<Array<Table>> {
    	const requestContextPromise = this.requestFactory.tablesList(schema, options);

		// build promise chain
    let middlewarePreObservable = from<RequestContext>(requestContextPromise);
    	for (let middleware of this.configuration.middleware) {
    		middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
    	}

    	return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
	    	pipe(mergeMap((response: ResponseContext) => {
	    		let middlewarePostObservable = of(response);
	    		for (let middleware of this.configuration.middleware) {
	    			middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
	    		}
	    		return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.tablesList(rsp)));
	    	}));
    }
	

}



