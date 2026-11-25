import { Component, OnInit, OnDestroy, ChangeDetectionStrategy, Input, HostListener } from '@angular/core';
import { BehaviorSubject, Observable, Subscription, combineLatest, merge, of } from 'rxjs';
import { distinctUntilChanged, filter, map, scan, shareReplay } from 'rxjs/operators';
import { StateService } from '@app/services/state.service';
import { WebsocketService } from '@app/services/websocket.service';
import { SeoService } from '@app/services/seo.service';
import { TransactionStripped } from '@interfaces/node-api.interface';
import { seoDescriptionNetwork } from '@app/shared/common.utils';

@Component({
  selector: 'app-recent-transactions-list',
  templateUrl: './recent-transactions-list.component.html',
  styleUrls: ['./recent-transactions-list.component.scss'],
  standalone: false,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RecentTransactionsList implements OnInit, OnDestroy {
  @Input() widget: boolean = false;

  transactions$: Observable<TransactionStripped[]>;
  bufferedCount$: Observable<number>;
  network$: Observable<string>;
  currency: string;
  currencySubscription: Subscription;
  isLoading = true;
  limitOptions = [10, 50, 100, 500, 1000];
  limit$: BehaviorSubject<number>;
  manualPaused$ = new BehaviorSubject<boolean>(false);
  autoPaused$ = new BehaviorSubject<boolean>(false);
  paused$: Observable<boolean>;

  constructor(
    public stateService: StateService,
    private websocketService: WebsocketService,
    private seoService: SeoService,
  ) {}

  ngOnInit(): void {
    this.limit$ = new BehaviorSubject<number>(this.widget ? 6 : 50);

    if (!this.widget) {
      this.websocketService.want(['stats', 'mempool-blocks']);
    }
    this.network$ = merge(of(''), this.stateService.networkChanged$);

    this.paused$ = combineLatest([this.manualPaused$, this.autoPaused$]).pipe(
      map(([m, a]) => m || a),
      distinctUntilChanged(),
    );

    // deduplicated FIFO queue of live transactions
    const accumulated$ = this.stateService.transactions$.pipe(
      filter((txs): txs is TransactionStripped[] => txs !== null && txs !== undefined),
      scan((acc, txs) => {
        // insert into map in chronological order
        for (let i = txs.length - 1; i >= 0; i--) {
          const tx = txs[i];
          if (!acc.has(tx.txid)) {
            acc.set(tx.txid, tx);
          }
        }
        while (acc.size > 1000) {
          acc.delete(acc.keys().next().value);
        }
        return acc;
      }, new Map<string, TransactionStripped>()), // ES6 maps preserve ordering
      map((acc) => Array.from(acc.values()).reverse()), // newest-first for rendering
      shareReplay({ bufferSize: 1, refCount: true }), // multicast to transactions$ + bufferedCount$, release source when unused
    );

    // while paused, keep the last visible snapshot so limit changes still apply
    const displayed$ = combineLatest([accumulated$, this.paused$]).pipe(
      scan((shown, [live, paused]) => (paused ? shown : live), [] as TransactionStripped[]),
    );

    this.transactions$ = combineLatest([displayed$, this.limit$]).pipe(
      map(([live, limit]) => live.slice(0, limit)),
    );

    // count new txs since pause by locating the newest tx seen at pause time (the queue is capped, so length alone can't grow)
    this.bufferedCount$ = combineLatest([accumulated$, this.paused$]).pipe(
      scan((state, [live, paused]) => {
        if (!paused) {
          return { frozen: false, newestTxid: null as string | null, count: 0 };
        }
        if (!state.frozen) {
          return { frozen: true, newestTxid: live[0]?.txid ?? null, count: 0 };
        }
        const idx = state.newestTxid === null ? -1 : live.findIndex((tx) => tx.txid === state.newestTxid);
        return { ...state, count: idx === -1 ? live.length : idx };
      }, { frozen: false, newestTxid: null as string | null, count: 0 }),
      map((state) => state.count),
      distinctUntilChanged(),
      shareReplay({ bufferSize: 1, refCount: true }),
    );

    this.currencySubscription = this.stateService.fiatCurrency$.subscribe((fiat) => {
      this.currency = fiat;
    });

    if (!this.widget) {
      this.seoService.setTitle($localize`:@@recent-transactions-title:Recent Transactions`);
      this.seoService.setDescription($localize`:@@meta.description.recent-transactions:See the most recent transactions on the Bitcoin${seoDescriptionNetwork(this.stateService.network)} network, updated in real-time.`);
    }
  }

  setLimit(limit: number): void {
    this.limit$.next(limit);
  }

  togglePause(): void {
    this.manualPaused$.next(!this.manualPaused$.value);
  }

  @HostListener('window:scroll')
  onWindowScroll(): void {
    if (this.widget || !this.stateService.isBrowser) {
      return;
    }
    const scrollEl = document.scrollingElement || document.documentElement;
    const atTop = !scrollEl || scrollEl.scrollTop <= 0;
    const shouldAutoPause = !atTop;
    if (shouldAutoPause === this.autoPaused$.value) {
      return;
    }
    this.autoPaused$.next(shouldAutoPause);
  }

  scrollToTop(): void {
    if (!this.stateService.isBrowser) {
      return;
    }
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  trackByTxid(index: number, tx: TransactionStripped): string {
    return tx.txid;
  }

  ngOnDestroy(): void {
    this.currencySubscription?.unsubscribe();
  }
}
