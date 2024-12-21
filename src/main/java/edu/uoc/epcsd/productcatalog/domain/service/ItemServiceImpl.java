package edu.uoc.epcsd.productcatalog.domain.service;

import edu.uoc.epcsd.productcatalog.domain.Item;
import edu.uoc.epcsd.productcatalog.domain.ItemStatus;
import edu.uoc.epcsd.productcatalog.domain.repository.ItemRepository;
import edu.uoc.epcsd.productcatalog.infrastructure.kafka.KafkaConstants;
import edu.uoc.epcsd.productcatalog.infrastructure.kafka.ProductMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Log4j2
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Service
public class ItemServiceImpl implements ItemService {

    private final ItemRepository itemRepository;

    private final KafkaTemplate<String, ProductMessage> productKafkaTemplate;

    @Override
    public List<Item> findAllItems() {
        return itemRepository.findAllItems();
    }

    public Optional<Item> findBySerialNumber(String serialNumber) {
        return itemRepository.findBySerialNumber(serialNumber);
    }

    @Override
    public List<Item> findByProductId(Long productId) {
        return itemRepository.findByProductId(productId);
    }

    public String createItem(Long productId, String serialNumber) {

        Item item = Item.builder().serialNumber(serialNumber).build();
        item.setProductId(productId);

        this.notifyProductAvailability(productId);

        return itemRepository.createItem(item);
    }

    private void notifyProductAvailability(Long productId) {
        productKafkaTemplate.send(KafkaConstants.PRODUCT_TOPIC + KafkaConstants.SEPARATOR + KafkaConstants.UNIT_AVAILABLE, ProductMessage.builder().productId(productId).build());
    }

    public Item setOperational(String serialNumber, boolean operational) {

        Item item = itemRepository.findBySerialNumber(serialNumber).orElseThrow(IllegalArgumentException::new);
        ItemStatus itemStatus = operational ? ItemStatus.OPERATIONAL : ItemStatus.NON_OPERATIONAL;

        // Change status only if the current status is different from the new status
        if (item.getStatus() != itemStatus) {
            item.setStatus(itemStatus);
            item = itemRepository.save(item);

            if (operational) {
                this.notifyProductAvailability(item.getProductId());
            }
        }
		return item;
    }

    @Override
    public void deleteItem(String serialNumber) {
        itemRepository.deleteItem(serialNumber);
    }

    @Override
    public void deleteItemsByProductId(Long productId) {
        List<Item> items = this.findByProductId(productId);
        items.forEach(item -> this.deleteItem(item.getSerialNumber()));
    }
}
