require 'rom/support/array_dataset'

module ROM
  module Memory
    # In-memory dataset
    #
    # @api public
    class Dataset
      include ArrayDataset

      # Join two datasets
      #
      # @api public
      def join(*args)
        left, right = args.size > 1 ? args : [self, args.first]

        join_map = left.each_with_object({}) { |tuple, h|
          others = right.to_a.find_all { |t| (tuple.to_a & t.to_a).any? }
          (h[tuple] ||= []).concat(others)
        }

        tuples = left.flat_map { |tuple|
          join_map[tuple].map { |other| tuple.merge(other) }
        }

        self.class.new(tuples, options)
      end

      # Restrict a dataset
      #
      # @api public
      def restrict(criteria = nil)
        if criteria
          find_all { |tuple| criteria.all? { |k, v| tuple[k].eql?(v) } }
        else
          find_all { |tuple| yield(tuple) }
        end
      end

      # Project a dataset
      #
      # @api public
      def project(*names)
        map { |tuple| tuple.reject { |key| !names.include?(key) } }
      end

      module Ordering
        Nils = {
          first: [
            ->(a, b) { a.nil? ? -1 : nil },
            ->(a, b) { b.nil? ? 1 : nil }
          ],
          last: [
            ->(a, b) { a.nil? ? 1 : nil },
            ->(a, b) { b.nil? ? -1 : nil }
          ]
        }

        class Compare
          def initialize(comparisons)
            @comparisons = comparisons + [:<=>.to_proc]
          end

          def call(a, b)
            comparisons.reduce(nil) do |result, comparison|
              result || comparison.call(a, b)
            end
          end

          private

          attr_reader :comparisons
        end
      end

      # Sort a dataset
      #
      # @api public
      def order(*names)
        options = names.last.is_a?(Hash) ? names.pop : { nils: :last }
        compare = Ordering::Compare.new(Ordering::Nils.fetch(options.fetch(:nils, :last)))

        sort do |left, right|
          names.reduce(0) do |order, name|
            if order.zero?
              compare.call(left[name], right[name])
            else
              order
            end
          end
        end
      end

      # Insert tuple into a dataset
      #
      # @api public
      def insert(tuple)
        data << tuple
        self
      end
      alias_method :<<, :insert

      # Delete tuples from a dataset
      #
      # @api public
      def delete(tuple)
        data.delete(tuple)
        self
      end
    end
  end
end
