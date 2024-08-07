export class InMemoryLock {
  private _isLocked = false;
      
  private waiting: (() => void)[] = [];

  isLocked(): boolean {
    return this._isLocked;
  }
	  
  async acquire(): Promise<void> {
    if (this._isLocked) {
      return new Promise(resolve => this.waiting.push(resolve));
    }
    this._isLocked = true;
  }
	
  release(): void {
    if (this.waiting.length > 0) {
      const next = this.waiting.shift();
      if (next) next();
    } else {
      this._isLocked = false;
    }
  }
}