using System;
using System.Threading;

namespace SharperAnorm
{
    internal class CallOnce<T>
    {
        private readonly Func<T> _f;
        private readonly Mutex _lock = new Mutex();
        private bool _completed;
        private T _result;

        public CallOnce(Func<T> f)
        {
            _f = f;
        }

        public T Call()
        {
            _lock.WaitOne();
            try
            {
                if (!_completed)
                {
                    _completed = true;
                    _result = _f();
                }

                return _result;
            }
            finally
            {
                _lock.ReleaseMutex();
            }
        }
    }
}