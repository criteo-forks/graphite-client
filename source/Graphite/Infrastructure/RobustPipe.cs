using System;
using System.Threading;

namespace Graphite.Infrastructure
{
    internal class RobustPipe : IPipe, IDisposable
    {
        private readonly Func<IPipe> pipeFactory;
        private readonly int retries;

        private IPipe innerPipe;
        private int consecutiveErrors;

        public RobustPipe(Func<IPipe> pipeFactory, int retries)
        {
            this.pipeFactory = pipeFactory;
            this.innerPipe = pipeFactory();
            this.retries = retries;
        }

        public bool Send(string message)
        {
            var success = this.innerPipe.Send(message);

            this.HandleReturnCode(success);

            return success;
        }

        public bool Send(string[] messages)
        {
            var success = this.innerPipe.Send(messages);

            this.HandleReturnCode(success);

            return success;
        }

        public void Dispose()
        {
            this.DisposePipe(this.innerPipe);
        }

        private void HandleReturnCode(bool success)
        {
            if (!success)
            {
                this.consecutiveErrors++;
            }
            else
            {
                this.consecutiveErrors = 0;
            }

            if (consecutiveErrors >= retries)
            {
                var oldPipe = Interlocked.Exchange(ref this.innerPipe, this.pipeFactory());
                this.DisposePipe(oldPipe);
                this.consecutiveErrors = 0;
            }
        }

        private void DisposePipe(IPipe pipe)
        {
            var disposablePipe = pipe as IDisposable;

            disposablePipe?.Dispose();
        }
    }
}
