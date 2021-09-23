package fdbs.sql.semantic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.attribute.*;
import fdbs.sql.parser.ast.clause.*;
import fdbs.sql.parser.ast.clause.boundary.Boundary;
import fdbs.sql.parser.ast.statement.*;
import fdbs.sql.parser.ast.constraint.*;
import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.literal.*;
import fdbs.sql.parser.ast.type.*;
import fdbs.sql.meta.*;
import fdbs.util.logger.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * A create table statement semantic validator which makes inter alia type
 * checks and integrity checks.
 */
public class CreateTableStatementSemanticValidator extends SemanticValidator {

    private HashMap<String, Type> attributeInfos = new HashMap<>();

    public CreateTableStatementSemanticValidator(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public void willVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Start create table statement semantic validation.", this.getClass().getSimpleName()));
    }

    @Override
    public boolean visit(ASTNode node) throws SemanticValidationException {
        boolean visitChilds = true;

        if (node instanceof CreateTableStatement) {
            CreateTableStatement createTableStmt = (CreateTableStatement) node;

            processTableName(createTableStmt.getTableIdentifier().getIdentifier(), false);
            processAttributes(createTableStmt.getAttributes());

            checkConstraint(createTableStmt.getConstraints());

            checkVerticalClause(createTableStmt.getVerticalClause());
            checkHorizontalClause(createTableStmt.getHorizontalClause());

            visitChilds = false;
        }

        return visitChilds;
    }

    @Override
    public void didVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Finished create table statement semantic validation.", this.getClass().getSimpleName()));
    }

    // <editor-fold defaultstate="collapsed" desc="Private Helper Functions">
    private void processAttributes(List<TableAttribute> attributes) throws SemanticValidationException {
        if (attributes != null && attributes.size() > 0) {
            for (Attribute attribute : attributes) {
                String name = attribute.getIdentifierNode().getIdentifier();
                Type type = attribute.getType();
                Literal defaultValueNode = attribute.getDefaultValue();

                if (defaultValueNode != null && !valueIsStrictTypeOf(defaultValueNode, type)) {
                    throw new SemanticValidationException(String.format("%s: The attriubte \"%s\" default value has a invalid type.", this.getClass().getSimpleName(), name));
                }

                if (attributeInfos.containsKey(name)) {
                    throw new SemanticValidationException(String.format("%s: The attriubte name \"%s\" defined multiple times.", this.getClass().getSimpleName(), name));
                }

                attributeInfos.put(name, type);
            }
        }
    }

    private boolean valueIsStrictTypeOf(Literal defaultValueLiteral, Type type) {
        boolean isTypeOf = false;

        if (type instanceof VarCharType && defaultValueLiteral instanceof StringLiteral) {
            isTypeOf = true;
        } else if (type instanceof PrimitiveType) {
            PrimitiveType primType = (PrimitiveType) type;
            if (primType.getPrimitive() == PrimitiveType.Primitive.INTEGER && defaultValueLiteral instanceof IntegerLiteral) {
                isTypeOf = true;
            }
        }

        return isTypeOf;
    }

    private void checkConstraint(List<fdbs.sql.parser.ast.constraint.Constraint> constraints) throws SemanticValidationException {
        HashSet<String> checkConstraintNames = new HashSet<>();

        for (fdbs.sql.parser.ast.constraint.Constraint constraint : constraints) {

            String constraintName = constraint.getConstraintIdentifier().getIdentifier();

            boolean constraintExists = metadataManager.getAllConstraints().stream().anyMatch((currConstraint) -> (currConstraint.getName().equals(constraintName)));
            if (constraintExists) {
                throw new SemanticValidationException(String.format("%s: A constraint with the name \"%s\" does already exists.", this.getClass().getSimpleName(), constraintName));
            }

            if (checkConstraintNames.contains(constraintName)) {
                throw new SemanticValidationException(String.format("%s: The constraint with the name \"%s\" does exists multiple times in this create table statement.", this.getClass().getSimpleName(), constraintName));
            }

            checkConstraintNames.add(constraintName);

            if (constraint instanceof CheckConstraint) {
                CheckCheckCoinstraint((CheckConstraint) constraint);
            } else if (constraint instanceof UniqueConstraint) {
                CheckUniqueConstraint((UniqueConstraint) constraint);
            } else if (constraint instanceof PrimaryKeyConstraint) {
                CheckPrimaryKeyConstraint((PrimaryKeyConstraint) constraint);
            } else if (constraint instanceof ForeignKeyConstraint) {
                CheckForeignKeyConstraint((ForeignKeyConstraint) constraint);
            } else {
                throw new SemanticValidationException(String.format("%s: Constraint \"%s\" has a unknown constraint type.", this.getClass().getSimpleName(), constraintName));
            }
        }
    }

    private void CheckUniqueConstraint(UniqueConstraint constraint) throws SemanticValidationException {
        String attributeName = constraint.getAttributeIdentifier().getIdentifier();
        if (!attributeInfos.containsKey(attributeName)) {
            throw new SemanticValidationException(String.format("%s: Unique Constraint \"%s\" - The attribute name \"%s\" was not definied.", this.getClass().getSimpleName(), constraint.getConstraintIdentifier().getIdentifier(), attributeName));
        }
    }

    private void CheckPrimaryKeyConstraint(PrimaryKeyConstraint constraint) throws SemanticValidationException {
        String attributeName = constraint.getAttributeIdentifier().getIdentifier();
        if (!attributeInfos.containsKey(attributeName)) {
            throw new SemanticValidationException(String.format("%s: Primary Key Constraint \"%s\" - The attribute name \"%s\" was not definied.", this.getClass().getSimpleName(), constraint.getConstraintIdentifier().getIdentifier(), attributeName));
        }
    }

    private void CheckForeignKeyConstraint(ForeignKeyConstraint constraint) throws SemanticValidationException {
        String constraintName = constraint.getConstraintIdentifier().getIdentifier();
        String attributeName = constraint.getAttributeIdentifier().getIdentifier();
        String refAttributeName = constraint.getReferencedAttribute().getIdentifier();
        String refTableName = constraint.getReferencedTable().getIdentifier();

        if (!attributeInfos.containsKey(attributeName)) {
            throw new SemanticValidationException(String.format("%s: Foreign Key Constraint \"%s\" - The attribute name \"%s\" was not definied.", this.getClass().getSimpleName(), constraintName, attributeName));
        }

        Table refTable = metadataManager.getTable(refTableName);
        if (refTable == null) {
            throw new SemanticValidationException(String.format("%s: Foreign Key Constraint \"%s\" - The referenced table \"%s\" does not exists.", this.getClass().getSimpleName(), constraintName, refTableName));
        }

        fdbs.sql.meta.Constraint primaryKeyConstraint = refTable.getPrimaryKeyConstraints().get(0);
        if (primaryKeyConstraint != null) {
            if (!primaryKeyConstraint.getAttributeName().equals(refAttributeName)) {
                throw new SemanticValidationException(String.format("%s: Foreign Key Constraint \"%s\" - The referenced attribute name \"%s\" does not exists in the referenced table \"%s\".", this.getClass().getSimpleName(), constraintName, refAttributeName, refTableName));
            }

            Type attributeType = attributeInfos.get(attributeName);
            Column column = refTable.getColumn(refAttributeName);
            String refAttributeType = column.getType();

            boolean invalidAttributeTypes = true;
            if (attributeType instanceof PrimitiveType) {
                if (((PrimitiveType) attributeType).getIdentifier().equals(refAttributeType)) {
                    invalidAttributeTypes = false;
                }
            } else if (attributeType instanceof VarCharType) {
                if (attributeType.getTypeDescriptor().equals(refAttributeType)) {
                    invalidAttributeTypes = false;
                }
            } else {
                throw new SemanticValidationException(String.format("%s: Foreign Key Constraint \"%s\" - The attribute type is unknown.", this.getClass().getSimpleName(), constraintName));
            }

            if (invalidAttributeTypes) {
                throw new SemanticValidationException(String.format("%s: Foreign Key Constraint \"%s\" - The attribute type is incompatible with the referenced attribute type.", this.getClass().getSimpleName(), constraintName));
            }
        } else {
            throw new SemanticValidationException(String.format("%s: Foreign Key Constraint \"%s\" - The referenced table has no primary key which can be refenced.", this.getClass().getSimpleName(), constraintName));
        }
    }

    private void CheckCheckCoinstraint(CheckConstraint constraint) throws SemanticValidationException {
        // type checking is not required because a oracle db allow to define invalid checks
        for (ASTNode node : constraint.getCheckExpression().getChildren()) {
            if (node instanceof AttributeIdentifier) {
                String attributeName = node.getIdentifier();
                if (!attributeInfos.containsKey(attributeName)) {
                    throw new SemanticValidationException(String.format("%s: Check Constraint \"%s\" - The attribute \"%s\" is unknown.", this.getClass().getSimpleName(), constraint.getConstraintIdentifier().getIdentifier(), attributeName));
                }
            }
        }
    }

    private void checkVerticalClause(ASTNode node) throws SemanticValidationException {
        if (node != null && node instanceof VerticalClause) {
            VerticalClause vClause = (VerticalClause) node;
            HashMap<String, String> checkedAttributes = new HashMap<>();

            processVerticalPartitioning(vClause.getAttributesForDB1(), "database 1", checkedAttributes);
            processVerticalPartitioning(vClause.getAttributesForDB2(), "database 2", checkedAttributes);
            processVerticalPartitioning(vClause.getAttributesForDB3(), "database 3", checkedAttributes);

            List<String> notUsedAttributes = new ArrayList<>();
            attributeInfos.keySet().stream().filter((identifier) -> (!checkedAttributes.keySet().contains(identifier))).forEachOrdered((identifier) -> {
                notUsedAttributes.add(identifier);
            });

            if (notUsedAttributes.size() > 0) {
                throw new SemanticValidationException(String.format("%s: Vertical Partitioning - The following attributes have no vertical partitioning assignment: %s.", this.getClass().getSimpleName(), String.join(";", notUsedAttributes)));
            }
        }
    }

    private void processVerticalPartitioning(List<AttributeIdentifier> attributes, String database, HashMap<String, String> checkedAttributes) throws SemanticValidationException {
        for (AttributeIdentifier identifier : attributes) {
            String attributeName = identifier.getIdentifier();
            if (!attributeInfos.containsKey(attributeName)) {
                throw new SemanticValidationException(String.format("%s: Vertical Partitioning - The attribute \"%s\" is unknown.", this.getClass().getSimpleName(), attributeName));
            }
            if (checkedAttributes.containsKey(attributeName)) {
                throw new SemanticValidationException(String.format("%s: Vertical Partitioning - The attribute \"%s\" is already defined for %s.", this.getClass().getSimpleName(), attributeName, checkedAttributes.get(attributeName)));
            }
            checkedAttributes.put(attributeName, database);
        }
    }

    private void checkHorizontalClause(ASTNode node) throws SemanticValidationException {
        if (node != null && node instanceof HorizontalClause) {
            HorizontalClause hClause = (HorizontalClause) node;
            String attributeName = hClause.getAttributeIdentifier().getIdentifier();

            if (!attributeInfos.containsKey(attributeName)) {
                throw new SemanticValidationException(String.format("%s: Horizontal Partitioning - The attribute \"%s\" is unknown.", this.getClass().getSimpleName(), attributeName));
            }

            Type attributeType = attributeInfos.get(attributeName);
            Boundary lowBoundary = hClause.getFirstBoundary();
            String lowBoundaryValue = null;
            if (lowBoundary != null) {
                Literal lowBoundaryLiteral = lowBoundary.getLiteral();
                lowBoundaryValue = lowBoundaryLiteral.getIdentifier();
                if (!valueIsStrictTypeOf(lowBoundaryLiteral, attributeType)) {
                    throw new SemanticValidationException(String.format("%s: Horizontal Partitioning - The first boundary value has a invalid type.", this.getClass().getSimpleName()));
                }
            }

            Literal highBoundaryLiteral = hClause.getSecondBoundary().getLiteral();
            String hightBoundaryValue = highBoundaryLiteral.getIdentifier();
            if (!valueIsStrictTypeOf(highBoundaryLiteral, attributeInfos.get(attributeName))) {
                String boundaryNumber = "first";
                if (lowBoundaryValue != null) {
                    boundaryNumber = "second";
                }
                throw new SemanticValidationException(String.format("%s: Horizontal Partitioning - The %s boundary value has a invalid type.", this.getClass().getSimpleName(), boundaryNumber));
            }

            if (lowBoundaryValue != null) {
                boolean boundariesAreValid = false;

                if (attributeType instanceof VarCharType) {
                    if (hightBoundaryValue.compareTo(lowBoundaryValue) > 0) {
                        boundariesAreValid = true;
                    }
                } else if (attributeType instanceof PrimitiveType) {
                    PrimitiveType primType = (PrimitiveType) attributeType;

                    if (primType.getPrimitive() == PrimitiveType.Primitive.INTEGER
                            && Integer.compare(Integer.parseInt(hightBoundaryValue), Integer.parseInt(lowBoundaryValue)) > 0) {
                        boundariesAreValid = true;
                    }
                } else {
                    throw new SemanticValidationException(String.format("%s: Horizontal Partitioning - The attribute type is invalid for horizontal partitioning.", this.getClass().getSimpleName()));
                }

                if (!boundariesAreValid) {
                    throw new SemanticValidationException(String.format("%s: Horizontal Partitioning - The first and the second are together invalid. The second boundary must be greater than the first boundary.", this.getClass().getSimpleName()));
                }
            }
        }
    }

// </editor-fold>
}
