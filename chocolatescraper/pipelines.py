import pg8000
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class PriceToUSDPipeline:
    gbpToUsdRate = 1.3

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('price'):
            float_price = float(adapter['price'])
            adapter['price'] = round(float_price * self.gbpToUsdRate, 2)
        else:
            spider.logger.warning(f'Missing price in {item}')
        return item


class PostgresPipeline:
    def __init__(self):
        self.conn = None
        self.curr = None

    def create_connection(self):
        try:
            if self.conn is None:
                self.conn = pg8000.connect(
                    host='node',
                    database='chocolate',
                    user='chocolate',
                    password='chocolate'
                )
                self.curr = self.conn.cursor()
        except pg8000.InterfaceError as e:
            raise Exception(f"Unable to connect to the database: {e}")

    def process_item(self, item, spider):
        self.create_connection()
        adapter = ItemAdapter(item)

        try:
            # Check for duplicates
            self.curr.execute("SELECT * FROM chocolate WHERE name = %s;", (adapter['name'],))
            if self.curr.fetchone():
                raise DropItem(f'Duplicate item found: {item!r}')

            # Insert new item
            self.curr.execute("INSERT INTO chocolate VALUES (%s, %s, %s);", (
                adapter['name'],
                adapter.get('price'),
                adapter['url']
            ))
            self.conn.commit()
            return item

        except pg8000.ProgrammingError as e:
            spider.logger.error(f"Database error: {e}")
            self.create_connection()  # Try to reconnect
            return item
        except Exception as e:
            spider.logger.error(f"Error processing item {item}: {e}")
            return item

    def close_spider(self, spider):
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                spider.logger.error(f"Error closing database connection: {e}")
