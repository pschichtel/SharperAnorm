using System;
using System.Threading.Tasks;

namespace SharperAnorm
{
    // TODO unit test this
    // TODO thread safety ?
    public class Rc<T>: IDisposable where T: class
    {
        private RcCell<T> _cell;

        public Rc(T obj, Func<T, Task> disposer) : this(new RcCell<T>(obj, disposer))
        {}

        private Rc(RcCell<T> cell)
        {
            _cell = cell;
            _cell.AddRef();
        }

        ~Rc()
        {
            // TODO docs say this might not be a good idea
            // See: https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/
            //  and https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-connection-pooling
            Dispose();
        }

        private RcCell<T> _getCell()
        {
            if (_cell == null)
            {
                throw new NullReferenceException("Reference already disposed!");
            }

            return _cell;
        }

        public Rc<T> Clone()
        {
            return new Rc<T>(_getCell());
        }

        public T Value => _getCell().Value;

        public void Dispose()
        {
            if (_cell != null)
            {
                _cell.DropRef();
                _cell = null;
                GC.SuppressFinalize(this);
            }
        }
    }

    internal class RcCell<T> where T: class
    {
        internal T Value { get; private set; }
        private Func<T, Task> _disposer;
        private int _refCount;

        public RcCell(T obj, Func<T, Task> disposer)
        {
            Value = obj;
            _disposer = disposer;
            _refCount = 0;
        }

        internal void AddRef()
        {
            _refCount++;
        }

        internal void DropRef()
        {
            _refCount--;
            if (_refCount == 0)
            {
                _disposer(Value);
                _disposer = null;
                Value = null;
            }
        }
    }
}